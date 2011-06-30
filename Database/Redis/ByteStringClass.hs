{-# LANGUAGE TypeSynonymInstances, FlexibleInstances #-}
module Database.Redis.ByteStringClass where

import Prelude hiding (concat)
import Data.ByteString
import Data.ByteString.Char8 (readInt)
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.UTF8 as U
import Data.Maybe (fromJust)

-- | Utility class for conversion to and from Strict ByteString
class BS a where
    toBS   :: a -> ByteString
    fromBS :: ByteString -> a

instance BS ByteString where
    toBS   = id
    fromBS = id

instance BS L.ByteString where
    toBS   = concat . L.toChunks
    fromBS = L.fromChunks . return

instance BS Char where
    toBS c = U.fromString [c]
    fromBS = Prelude.head . U.toString

instance BS String where
    toBS   = U.fromString
    fromBS = U.toString

instance BS Int where
    toBS   = U.fromString . show
    fromBS = fst . fromJust . readInt

instance BS Double where
    toBS   = U.fromString . show
    fromBS = read . U.toString

instance BS () where
    toBS   = const empty
    fromBS = const ()
