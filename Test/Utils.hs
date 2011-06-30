{-# LANGUAGE CPP #-}
module Test.Utils where

import System.Random
import Control.Monad
import Test.HUnit
import Control.Exception
import Control.Concurrent
import Control.Concurrent.MVar
import Database.Redis.Redis
import Database.Redis.ByteStringClass

#if defined(__GLASGOW_HASKELL__) && __GLASGOW_HASKELL__ >= 610
assertRaises :: (Show a, Control.Exception.Exception e, Show e) =>
                String -> e -> IO a -> IO ()
#else
assertRaises :: Show a => String -> Control.Exception.Exception -> IO a -> IO ()
#endif
assertRaises msg selector action =
    let {- thetest e = if e == selector then return ()
                    else assertFailure $ msg ++ "\nReceived unexpected exception: "
                             ++ (show e) ++ "\ninstead of exception: " ++ (show selector)
                             -}
        thetest :: a -> a -> IO ()
        thetest _ e = return ()
        in do r <- Control.Exception.try action
              case r of
                Left e -> thetest selector e
                Right _ -> assertFailure $ msg ++ "\nReceived no exception, but was expecting exception: " ++ (show selector)

assertOneOf :: (Eq a, Show a) => String -> [a] -> a -> Assertion
assertOneOf msg lst a = find lst
    where find [] = assertFailure (msg ++ "\n" ++ show a ++ " not found in " ++ show lst)
          find (x:xs) = if a == x
                        then return ()
                        else find xs

assertRError :: BS a => String -> Reply a -> Assertion
assertRError msg (RError _) = return ()
assertRError msg r = assertFailure $ msg ++ "\n" ++ show r ++ " is not a RError"

assertRQueued :: BS a => String -> Reply a -> Assertion
assertRQueued msg RQueued = return ()
assertRQueued msg r = assertFailure $ msg ++ "\n" ++ show r ++ " is not a RQueued"

{------------------ Concurrent ------------------}
later :: Int -> IO a -> IO (MVar a)
later t a = do v <- newEmptyMVar
               forkIO $ threadDelay t >> a >>= putMVar v
               return v

{-------------------- Random --------------------}
(.:.) = liftM2 (:)
infixr 5 .:.

randomRIOs _ 0 = return []
randomRIOs range n = randomRIO range .:. randomRIOs range (n-1)
