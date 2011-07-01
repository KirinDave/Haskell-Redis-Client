module Data.Enumerator.Redis (enumSubscriptions) where
       
import Database.Redis.Redis       
import Data.Enumerator ((>>==), Enumerator, Stream(..), Step(..), returnI)
import Data.Maybe (maybeToList)
import Control.Concurrent.MonadIO (MonadIO, liftIO)
import Database.Redis.ByteStringClass


enumSubscriptions :: (BS s, MonadIO m) =>
                          Redis 
                       -> Int
                       -> Enumerator (Message s) m b
enumSubscriptions rs timeout (Continue k) = do
  nextRead <- liftIO $ listen rs timeout;
  let chunk = Chunks $ maybeToList nextRead in
    k chunk >>== subscriptionEnumerator rs timeout
subscriptionEnumerator rs timeout step = returnI step
      