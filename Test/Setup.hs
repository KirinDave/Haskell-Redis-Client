{-# LANGUAGE FlexibleContexts, PackageImports #-}
module Test.Setup (
  startRedis,
  shutdownRedis,
  testRedis, testRedis2,
  ask2, addStr, addList,
  addSet, addZSet,
  addHash, addAll,

  RedisM, runWithRedis,

  Opts(..),
  T(..), pushParam, toHUnit,

  module Control.Exception,
  module Control.Concurrent,
  module Control.Monad.Reader,
  module Data.Maybe,
  module Database.Redis.Redis,
  module Database.Redis.ByteStringClass,
  module Database.Redis.Utils.Lock,
  module System.Cmd,
  module System.Directory,
  module System.FilePath,
  module System.Time,
  module Test.HUnit,
  module Test.Utils
  ) where

import Control.Exception hiding (assert, throwTo)
import Control.Concurrent
import "mtl" Control.Monad.Reader
import Data.Maybe
import Database.Redis.Monad.State (RedisM, runWithRedis)
import Database.Redis.Redis
import Database.Redis.ByteStringClass
import Database.Redis.Utils.Lock
import System.Cmd
import System.Directory
import System.FilePath
import System.Time
import Test.HUnit
import Test.Utils

data Opts = Opts { optHost :: String,
                   optPort :: String,
                   optDatabase :: Int,
                   optBinary :: Maybe String,
                   optConfig :: Maybe String,
                   optHelp :: Bool }
            deriving Show

data T a = TCase { runTCase :: a }
         | TLabel String (T a)
         | TList [T a]

pushParam :: a -> T (a -> b) -> T b
pushParam a (TCase f) = TCase $ f a
pushParam a (TLabel l t) = TLabel l $ pushParam a t
pushParam a (TList ts) = TList $ map (pushParam a) ts

toHUnit :: T (IO ()) -> Test
toHUnit (TCase t) = TestCase $ t
toHUnit (TLabel s t) = TestLabel s $ toHUnit t
toHUnit (TList ts) = TestList $ map toHUnit ts

startRedis :: FilePath -> FilePath -> IO ()
startRedis path_to_redis path_to_config = do system $ path_to_redis ++ " " ++ path_to_config
                                             threadDelay $ 10000
                                             return ()

shutdownRedis = do r <- connect localhost defaultPort
                   shutdown r
                   return ()

testRedis :: (ReaderT Redis IO ()) -> Opts -> IO ()
testRedis t opts = bracket setup teardown $ runReaderT t
    where setup = do r <- connect (optHost opts) (optPort opts)
                     select r $ optDatabase opts
                     return r
          teardown r = do flushDb r
                          disconnect r

testRedis2 :: (ReaderT Redis (ReaderT Redis IO) ()) -> Opts -> IO ()
testRedis2 t opts = bracket setup teardown $ \(r1, r2) -> runReaderT (runReaderT t r2) r1
    where setup = do r1 <- connect (optHost opts) (optPort opts)
                     select r1 $ optDatabase opts
                     r2 <- connect (optHost opts) (optPort opts)
                     select r2 $ optDatabase opts
                     return (r1, r2)
          teardown (r1, r2) = do flushDb r1
                                 disconnect r1
                                 disconnect r2

ask2 :: (MonadReader a m, MonadTrans t) => t m a
ask2 = lift ask

addStr :: (MonadReader Redis m, MonadIO m) => m ()
addStr = do r <- ask
            liftIO $ do set r "foo" "foo"
                        set r "bar" "bar"
            return ()

addList :: (MonadReader Redis m, MonadIO m) => m ()
addList = do r <- ask
             liftIO $ mapM_ (rpush r "list") ["1", "2", "3"]


addSet :: (MonadReader Redis m, MonadIO m) => m ()
addSet = do r <- ask
            liftIO $ mapM_ (sadd r "set") ["1", "2", "3"]

addZSet :: (MonadReader Redis m, MonadIO m) => m ()
addZSet = do r <- ask
             liftIO $ mapM_ (uncurry (zadd r "zset")) $ zip (reverse [1.0, 2.0, 3.0, 4.0, 5.0]) ["1", "2", "3", "4", "5"]

addHash :: (MonadReader Redis m, MonadIO m) => m ()
addHash = do r <- ask
             liftIO $ mapM_ (uncurry (hset r "hash")) $ zip ["foo", "bar", "baz"] ["1", "2", "3"]

addAll :: (MonadReader Redis m, MonadIO m) => m ()
addAll = addStr >> addList >> addSet >> addZSet >> addHash
