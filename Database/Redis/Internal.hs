{-# LANGUAGE CPP, OverloadedStrings #-}
module Database.Redis.Internal where

import Prelude hiding (putStrLn, putStr, catch)
import Control.Concurrent (ThreadId, myThreadId)
import Control.Concurrent.MVar
import Data.IORef
import qualified System.IO as IO
import System.IO.UTF8 (putStrLn, putStr)
import qualified Data.ByteString as B
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (readInt)
import qualified Data.ByteString.UTF8 as U
import Data.Maybe (fromJust, isNothing, isJust)
import Data.List (intersperse)
import qualified Data.Map as Map
import Data.Map (Map(..))
import Control.Monad (when)
import Control.Exception (bracket, bracketOnError, catch, SomeException)

import Database.Redis.ByteStringClass

#if __GLASGOW_HASKELL__ < 700
import Control.Exception (block)
#else
import Control.Exception.Base (mask)
block f = mask $ \ _ -> f
#endif

tracebs = putStrLn . U.toString
tracebs' = putStr . U.toString

data RedisState = RedisState { server :: (String, String), -- ^ hostname and port pair
                               database :: Int,            -- ^ currently selected database
                               handle :: IO.Handle,        -- ^ real network connection
                               isSubscribed :: Int,        -- ^ currently in PUB/SUB mode
                               renamedCommands :: Map ByteString ByteString -- ^ map of the renamed commands
                             }

-- | Redis connection descriptor
data Redis = Redis {r_lock_cnt :: MVar (Maybe (ThreadId, Int)),
                    r_lock     :: MVar (),
                    r_st       :: IORef RedisState}
             deriving Eq

newRedis :: (String, String) -> IO.Handle -> IO Redis
newRedis server h = do lcnt <- newMVar Nothing
                       l <- newMVar ()
                       st <- newIORef $ RedisState server 0 h 0 Map.empty
                       return $ Redis lcnt l st

-- | Redis command variants
data Command = CInline ByteString
             | CMInline [ByteString]
             | CBulk [ByteString] ByteString
             | CMBulk [ByteString]

-- | Redis reply variants
data BS s => Reply s = RTimeout               -- ^ Timeout. Currently unused
                     | RParseError String     -- ^ Error converting value from ByteString. It's a client-side error.
                     | ROk                    -- ^ \"Ok\" reply
                     | RPong                  -- ^ Reply for the ping command
                     | RQueued                -- ^ Used inside multi-exec block
                     | RError String          -- ^ Some kind of server-side error
                     | RInline s              -- ^ Simple oneline reply
                     | RInt Int               -- ^ Integer reply
                     | RBulk (Maybe s)        -- ^ Multiline reply
                     | RMulti (Maybe [Reply s]) -- ^ Complex reply. It may consists of various type of replys
                       deriving Eq

showbs :: BS s => s -> String
showbs = U.toString . toBS

instance BS s => Show (Reply s) where
    show RTimeout = "RTimeout"
    show (RParseError msg) = "RParseError: " ++ msg
    show ROk = "ROk"
    show RPong = "RPong"
    show RQueued = "RQueued"
    show (RError msg) = "RError: " ++ msg
    show (RInline s) = "RInline (" ++ (showbs s) ++ ")"
    show (RInt a) = "RInt " ++ show a
    show (RBulk (Just s)) = "RBulk " ++ showbs s
    show (RBulk Nothing) = "RBulk Nil"
    show (RMulti (Just rs)) = "RMulti [" ++ join rs ++ "]"
                              where join = concat . intersperse ", " . map show
    show (RMulti Nothing) = "RMulti Nil"

data (BS s) => Message s = MSubscribe s Int   -- ^ subscribed
                         | MUnsubscribe s Int -- ^ unsubscribed
                         | MPSubscribe s Int  -- ^ pattern subscribed
                         | MPUnsubscribe s Int -- ^ pattern unsubscribed
                         | MMessage s s        -- ^ message recieved
                         | MPMessage s s s     -- ^ message recieved by pattern
                           deriving (Eq, Show)

urn       = U.fromString "\r\n"
uspace    = U.fromString " "
uminus    = U.fromString "-"
uplus     = U.fromString "+"
ucolon    = U.fromString ":"
ubucks    = U.fromString "$"
uasterisk = U.fromString "*"

hPutRn h = B.hPut h urn
{-# INLINE hPutRn #-}

takeState :: Redis -> IO RedisState
takeState r = block $ do lcnt <- takeMVar $ r_lock_cnt r
                         mytid  <- myThreadId
                         case lcnt of
                           Nothing -> do l <- tryTakeMVar $ r_lock r
                                         when (isNothing l)
                                                  $ error "takeState: r_lock_cnt is Nothing BUT r_lock is locked"
                                         take_n_put mytid 1
                           Just (tid, cnt) -> if tid == mytid
                                              then let !cnt' = cnt + 1
                                                   in take_n_put tid cnt'
                                              else do putMVar (r_lock_cnt r) lcnt
                                                      l <- takeMVar $ r_lock r
                                                      lcnt <- takeMVar $ r_lock_cnt r
                                                      when (isJust lcnt)
                                                               $ error "takeState: r_lock is locked by me BUT r_lock_cnt is not Nothing"
                                                      take_n_put mytid 1
    where take_n_put tid cnt = do st <- readIORef $ r_st r
                                  putMVar (r_lock_cnt r) $ Just (tid, cnt)
                                  return st

putState :: Redis -> RedisState -> IO ()
putState r s = block $ do lcnt <- takeMVar $ r_lock_cnt r
                          mytid <- myThreadId
                          case lcnt of
                            Nothing -> error "putState: trying put state that was not took"
                            Just (tid, cnt) -> if tid /= mytid
                                               then error "putState: trying put state that was not took by me"
                                               else do writeIORef (r_st r) s
                                                       if cnt > 1
                                                         then let !cnt' = cnt - 1
                                                              in putMVar (r_lock_cnt r) $ Just (tid, cnt')
                                                         else do putMVar (r_lock r) ()
                                                                 putMVar (r_lock_cnt r) Nothing

putStateUnmodified :: Redis -> IO ()
putStateUnmodified r = block $ do lcnt <- takeMVar $ r_lock_cnt r
                                  mytid <- myThreadId
                                  case lcnt of
                                    Nothing -> error "putState: trying put state that was not took"
                                    Just (tid, cnt) -> if tid /= mytid
                                                       then error "putState: trying put state that was not took by me"
                                                       else if cnt > 1
                                                            then let !cnt' = cnt - 1
                                                                 in putMVar (r_lock_cnt r) $ Just (tid, cnt')
                                                            else do putMVar (r_lock r) ()
                                                                    putMVar (r_lock_cnt r) Nothing

inState :: Redis -> (RedisState -> IO (RedisState, a)) -> IO a
inState r action = bracketOnError (takeState r) (\_ -> putStateUnmodified r)
                     $ \s -> do (s', a) <- action s
                                putState r s'
                                return a

inState_ :: Redis -> (RedisState -> IO RedisState) -> IO ()
inState_ r action = bracketOnError (takeState r) (\_ -> putStateUnmodified r) (\s -> action s >>= putState r)

withState :: Redis -> (RedisState -> IO a) -> IO a
withState r action = bracket (takeState r) (\_ -> putStateUnmodified r) action

withState' = flip withState

send :: IO.Handle -> [ByteString] -> IO ()
send h [] = return ()
send h (bs:ls) = B.hPut h bs >> B.hPut h uspace >> send h ls

lookupRenamed :: RedisState -> ByteString -> ByteString
lookupRenamed r c = let c' = Map.findWithDefault c c (renamedCommands r)
                    in if B.null c'
                         then error $ "Command " ++ (fromBS c :: String) ++ " is disabled"
                         else c'

sendCommand :: RedisState -> Command -> IO ()
sendCommand r (CInline bs) = let h = handle r
                                 cmd = lookupRenamed r bs
                             in B.hPut h cmd >> hPutRn h >> IO.hFlush h
sendCommand r (CMInline (l:ls)) = let h = handle r
                                      cmd = lookupRenamed r l
                                  in send h (cmd:ls) >> hPutRn h >> IO.hFlush h
sendCommand r (CBulk (l:ls) bs) = let h = handle r
                                      size = U.fromString $ show $ B.length bs
                                      cmd = lookupRenamed r l
                                  in do send h (cmd:ls)
                                        B.hPut h uspace
                                        B.hPut h size
                                        hPutRn h
                                        B.hPut h bs
                                        hPutRn h
                                        IO.hFlush h
sendCommand r (CMBulk s@(c:cs)) = let h = handle r
                                      sendls [] = return ()
                                      sendls (bs:ls) = let size = U.fromString . show . B.length
                                                       in do B.hPut h ubucks
                                                             B.hPut h $ size bs
                                                             hPutRn h
                                                             B.hPut h bs
                                                             hPutRn h
                                                             sendls ls
                                      c' = lookupRenamed r c
                                  in do B.hPut h uasterisk
                                        B.hPut h $ U.fromString $ show $ length s
                                        hPutRn h
                                        sendls (c':cs)
                                        IO.hFlush h

sendCommand' = flip sendCommand

recv :: BS s => RedisState -> IO (Reply s)
recv r = do first <- trim `fmap` B.hGetLine h
            case U.uncons first of
              Just ('-', rest)  -> recv_err rest
              Just ('+', rest)  -> recv_inline rest
              Just (':', rest)  -> recv_int rest
              Just ('$', rest)  -> recv_bulk rest
              Just ('*', rest)  -> recv_multi rest
    where
      h = handle r
      trim = B.takeWhile (\c -> c /= 13 && c /= 10)

      safeFromBS constructor bs = (return $! constructor $! fromBS bs)
                                  `catch`
                                  (\e -> let msg = show (e :: SomeException)
                                         in return $ RParseError msg)

      -- recv_err :: ByteString -> IO Reply
      recv_err rest = return $ RError $ U.toString rest

      -- recv_inline :: ByteString -> IO Reply
      recv_inline rest = case rest of
                           "OK"       -> return ROk
                           "PONG"     -> return RPong
                           "QUEUED"   -> return RQueued
                           _          -> safeFromBS RInline rest

      -- recv_int :: ByteString -> IO Reply
      recv_int rest = let reply = fst $ fromJust $ readInt rest
                      in return $ RInt reply

      -- recv_bulk :: ByteString -> IO Reply
      recv_bulk rest = let size = fst $ fromJust $ readInt rest
                       in do body <- recv_bulk_body size
                             maybe (return $ RBulk Nothing) (safeFromBS (RBulk . Just)) body

      -- recv_bulk_body :: Int -> IO (Maybe ByteString)
      recv_bulk_body (-1) = return Nothing
      recv_bulk_body size = do body <- B.hGet h (size + 2)
                               let reply = B.take size body
                               return $ Just reply

      -- recv_multi :: ByteString -> IO Reply
      recv_multi rest = let cnt = fst $ fromJust $ readInt rest
                        in do bulks <- recv_multi_n cnt
                              return $ RMulti bulks

      -- recv_multi_n :: Int -> IO (Maybe [Reply])
      recv_multi_n (-1) = return Nothing
      recv_multi_n 0    = return $ Just []
      recv_multi_n n = do this <- recv r
                          tail <- fromJust `fmap` recv_multi_n (n-1)
                          return $ Just (this : tail)

wait :: RedisState -> Int -> IO Bool
wait rs = IO.hWaitForInput (handle rs)
