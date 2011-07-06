module Data.Enumerator.Redis (enumSubscriptions) where
       
import Database.Redis.Redis       
import Data.Enumerator ((>>==), Enumerator, Stream(..), continue, checkContinue0)
import Control.Concurrent.MonadIO (MonadIO, liftIO)
import Data.Maybe (maybeToList)
import Database.Redis.ByteStringClass
import qualified Data.ByteString as B


enumSubscriptions :: (BS s, MonadIO m) =>
                     Int
                     -> Redis 
                     -> Enumerator (Message s) m b
enumSubscriptions timeout rs = checkContinue0 $ \loop k -> 
  do nextRead <- liftIO $ listen rs timeout;
     k (Chunks $ maybeToList nextRead) >>== loop      