module Data.Enumerator.Redis (enumSubscriptions) where
       
import Database.Redis.Redis       
import Data.Enumerator ((>>==), Enumerator, Stream(..), continue, checkContinue0)
import Data.Maybe (fromMaybe)
import Control.Concurrent.MonadIO (MonadIO, liftIO)
import Database.Redis.ByteStringClass
import qualified Data.ByteString as B

{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, FunctionalDependencies,
  FlexibleContexts, OverloadedStrings #-}



enumSubscriptions :: (BS s, MonadIO m) =>
                     Int
                     -> Redis 
                     -> Enumerator (Message s) m b
enumSubscriptions timeout rs = checkContinue0 $ \loop k -> 
  do nextRead <- liftIO $ listen rs timeout;
     case nextRead of
       Nothing -> k (Chunks [])  >>== loop
       Just x  -> k (Chunks [x]) >>== loop
      