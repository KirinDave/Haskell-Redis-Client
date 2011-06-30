{-
Copyright (c) 2010 Alexander Bogdanov <andorn@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
-}

-- | Same as "Database.Redis.Utils.Lock" but with monadic wrapped
module Database.Redis.Utils.Monad.Lock (acquire, acquire', acquireOnce, release) where

import Control.Monad.Trans
import qualified Database.Redis.Utils.Lock as L
import Database.Redis.Monad (WithRedis(..))
import Database.Redis.ByteStringClass

acquire :: (WithRedis m, BS s) => s -> Int -> Int -> m Bool
acquire name timeout retry_timeout =
    do r <- getRedis
       liftIO $ L.acquire r name timeout retry_timeout

acquire' name timeout = acquire name timeout 1

acquireOnce name = getRedis >>= liftIO . flip L.acquireOnce name

release :: (WithRedis m, BS s) => s -> m ()
release name = getRedis >>= liftIO . flip L.release name
