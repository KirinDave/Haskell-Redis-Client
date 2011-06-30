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

-- | Emulating locking primitives
module Database.Redis.Utils.Lock ( acquire, acquire', acquireOnce, release )
where

import Control.Concurrent (threadDelay)
import Database.Redis.Redis
import Database.Redis.ByteStringClass
import System.Time

-- | Acquire lock. This function is not reentrant so thread can be
-- locked by itself if it try to acquire the same lock before it was
-- released.
acquire :: BS s =>
           Redis
        -> s            -- ^ The lock's name
        -> Int          -- ^ Timeout in milliseconds.
        -> Int          -- ^ Time interval between attempts to lock on
        -> IO Bool      -- ^ True if lock was acquired
acquire r name 0 _ = acquireOnce r name
acquire r name timeout retry_timeout = do res <- acquireOnce r name
                                          if res then return True else getClockTime >>= trylock

    where trylock t = do now <- getClockTime
                         if diffClockTimesMs t now >= timeout
                           then return False
                           else do res <- acquireOnce r name
                                   if res
                                     then return True
                                     else threadDelay rt >> trylock t

          rt = retry_timeout * 1000

-- | acquire with default last parameter set to 50 milliseconds
acquire' r name timeout = acquire r name timeout 50

-- | Try to acquire lock once and return result without any timeout
acquireOnce r name = do res <- setNx r name "1" >>= fromRInt
                        return (res == 1)

-- | Release lock. There is no any guarantees that lock was acquired
-- in this thread. Just release this lock and go forth.
release :: BS s => Redis -> s -> IO ()
release r name = del r name >>= noError

diffClockTimesMs :: ClockTime -> ClockTime -> Int
diffClockTimesMs (TOD t1s t1m) (TOD t2s t2m) = fromIntegral $ (t2s - t1s) * 1000 + (t2m - t1m) `div` 1000000000
