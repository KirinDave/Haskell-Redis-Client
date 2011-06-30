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

{-# LANGUAGE FlexibleContexts, OverloadedStrings, PackageImports #-}

-- | Monadic wrapper for "Database.Redis.Redis"
module Database.Redis.Monad (
       -- * Types ans Constructors
       WithRedis(..),
       R.Redis(..),
       R.Reply(..),
       R.Message(..),
       R.LInsertDirection(..),
       R.Interval(..),
       R.IsInterval(..),
       R.SortOptions(..),
       R.Aggregate(..),
       R.RedisKeyType,
       R.RedisInfo,
       R.sortDefaults,

       R.fromRInline, R.fromRBulk, R.fromRMulti, R.fromRMultiBulk,
       R.fromRInt, R.fromROk, R.noError, R.parseMessage, R.takeAll,

       -- * Database connection
        R.localhost, R.defaultPort,
       connect, disconnect, isConnected,
       getServer, getDatabase, renameCommand,

       -- * Redis commands
       -- ** Generic
       ping, auth, echo, quit, shutdown,
       multi, exec, discard, run_multi,
       watch, unwatch, run_cas, exists,
       del, getType, keys, randomKey, rename,
       renameNx, dbsize, expire, expireAt,
       persist, ttl, select, move, flushDb,
       flushAll, info,

       -- ** Strings
       set, setNx,setEx, mSet, mSetNx,
       get, getSet, mGet,
       incr, incrBy, decr,
       decrBy, append, substr,
       getrange, setrange,
       getbit, setbit,strlen,

       -- ** Lists
       rpush, lpush, rpushx, lpushx,
       llen, lrange, ltrim,
       lindex, lset, lrem, lpop, rpop,
       rpoplpush, blpop, brpop, brpoplpush,

       -- ** Sets
       sadd, srem, spop, smove, scard, sismember,
       smembers, srandmember, sinter, sinterStore,
       sunion, sunionStore, sdiff, sdiffStore,

       -- ** Sorted sets
       zadd, zrem, zincrBy, zrange,
       zrevrange, zrangebyscore, zrevrangebyscore,
       zcount, zremrangebyscore, zcard, zscore,
       zrank, zrevrank, zremrangebyrank,
       zunion, zinter, zunionStore, zinterStore,

       -- ** Hashes
       hset, hget, hdel, hmset, hmget,
       hincrby, hexists, hlen,
       hkeys, hvals, hgetall,

       -- ** Sorting
       sort, listRelated,

       -- ** Publish/Subscribe
       subscribed, subscribe, unsubscribe,
       psubscribe, punsubscribe, publish,
       listen,

       -- ** Persistent control
       save, bgsave, lastsave, bgrewriteaof
)
where

import "mtl" Control.Monad.Trans
import "MonadCatchIO-mtl" Control.Monad.CatchIO
import Data.ByteString (ByteString)

import qualified Database.Redis.Redis as R
import Database.Redis.Redis (IsInterval)
import Database.Redis.ByteStringClass
import qualified Database.Redis.Internal as Internal

class MonadIO m => WithRedis m where
    getRedis :: m (R.Redis)
    setRedis :: R.Redis -> m ()

connect :: WithRedis m => String -> String -> m ()
connect host port = liftIO (R.connect host port) >>= setRedis

disconnect :: WithRedis m => m ()
disconnect = getRedis >>= liftIO . R.disconnect

isConnected :: WithRedis m => m Bool
isConnected = getRedis >>= liftIO . R.isConnected

getServer :: WithRedis m => m (String, String)
getServer = getRedis >>= liftIO . R.getServer

getDatabase :: WithRedis m => m Int
getDatabase = getRedis >>= liftIO . R.getDatabase

renameCommand :: WithRedis m => ByteString -> ByteString -> m ()
renameCommand c c' = do r <- getRedis
                        liftIO $ R.renameCommand r c c'

ping :: WithRedis m => m (R.Reply ())
ping = getRedis >>= liftIO . R.ping

auth :: WithRedis m => String -> m (R.Reply ())
auth pwd = getRedis >>= liftIO . flip R.auth pwd

echo :: (WithRedis m, BS s) => s -> m (R.Reply s)
echo s = getRedis >>= liftIO . flip R.echo s

quit :: WithRedis m => m ()
quit = getRedis >>= liftIO . R.quit

shutdown :: WithRedis m => m ()
shutdown = getRedis >>= liftIO . R.shutdown

multi :: WithRedis m => m (R.Reply ())
multi = getRedis >>= liftIO . R.multi

exec :: (WithRedis m, BS s) => m (R.Reply s)
exec = getRedis >>= liftIO . R.exec

discard :: WithRedis m => m (R.Reply ())
discard = getRedis >>= liftIO . R.discard

run_multi :: (MonadCatchIO m, WithRedis m, BS s) => m () -> m (R.Reply s)
run_multi cs = do r <- getRedis
                  bracket (liftIO $ Internal.takeState r)
                          (\_ -> liftIO $ Internal.putStateUnmodified r)
                          (\rs -> do liftIO $ (Internal.sendCommand rs (Internal.CInline "MULTI") >> (Internal.recv rs :: IO (Internal.Reply ())))
                                     cs `onException` (liftIO $ do Internal.sendCommand rs (Internal.CInline "DISCARD")
                                                                   Internal.recv rs :: IO (Internal.Reply ()))
                                     liftIO $ Internal.sendCommand rs (Internal.CInline "EXEC")
                                     liftIO $ Internal.recv rs)

watch :: (WithRedis m, BS s) => [s] -> m (R.Reply ())
watch cs = getRedis >>= liftIO . flip R.watch cs

unwatch :: WithRedis m => m (R.Reply ())
unwatch = getRedis >>= liftIO . R.unwatch

run_cas :: (MonadCatchIO m, WithRedis m, BS s1) => [s1] -> m a -> m a
run_cas keys cs = let keys' = map toBS keys
                  in do r <- getRedis
                        bracket (liftIO $ Internal.takeState r)
                                (\_ -> liftIO $ Internal.putStateUnmodified r)
                                (\rs -> do liftIO $ (Internal.sendCommand rs (Internal.CMBulk ("WATCH" : keys')))
                                                      >> (Internal.recv rs :: IO (Internal.Reply ()))
                                                      >>= R.fromROk
                                           res <- cs `onException` (liftIO $ do Internal.sendCommand rs (Internal.CInline "DISCARD")
                                                                                Internal.recv rs :: IO (Internal.Reply ())
                                                                                Internal.sendCommand rs (Internal.CInline "UNWATCH")
                                                                                Internal.recv rs :: IO (Internal.Reply ()))
                                           liftIO $ (Internal.sendCommand rs (Internal.CInline "UNWATCH"))
                                                      >> (Internal.recv rs :: IO (Internal.Reply ()))
                                                      >>= R.fromROk
                                           return res)

exists :: (WithRedis m, BS s) => s -> m (R.Reply Int)
exists key = getRedis >>= liftIO . flip R.exists key

del :: (WithRedis m, BS s) => s -> m (R.Reply Int)
del key = getRedis >>= liftIO . flip R.del key

getType :: (WithRedis m, BS s1) => s1 -> m R.RedisKeyType
getType key = getRedis >>= liftIO . flip R.getType key

keys :: (WithRedis m, BS s1, BS s2) => s1 -> m (R.Reply s2)
keys pattern = getRedis >>= liftIO . flip R.keys pattern

randomKey :: (WithRedis m, BS s) => m (R.Reply s)
randomKey = getRedis >>= liftIO . R.randomKey

rename :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply ())
rename from to = do r <- getRedis
                    liftIO $ R.rename r from to

renameNx :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
renameNx from to = do r <- getRedis
                      liftIO $ R.renameNx r from to

dbsize :: WithRedis m => m (R.Reply Int)
dbsize = getRedis >>= liftIO . R.dbsize

expire :: (WithRedis m, BS s) => s -> Int -> m (R.Reply Int)
expire key seconds = do r <- getRedis
                        liftIO $ R.expire r key seconds

expireAt :: (WithRedis m, BS s) => s -> Int -> m (R.Reply Int)
expireAt key timestamp = do r <- getRedis
                            liftIO $ R.expireAt r key timestamp

persist :: (WithRedis m, BS s) => s -> m (R.Reply Int)
persist key = getRedis >>= liftIO . flip R.persist key

ttl :: (WithRedis m, BS s) => s -> m (R.Reply Int)
ttl key = getRedis >>= liftIO . flip R.ttl key

select :: WithRedis m => Int -> m (R.Reply ())
select db = getRedis >>= liftIO . flip R.select db

move :: (WithRedis m, BS s) => s -> Int -> m (R.Reply Int)
move key db = do r <- getRedis
                 liftIO $ R.move r key db

flushDb :: WithRedis m => m (R.Reply ())
flushDb = getRedis >>= liftIO . R.flushDb

flushAll :: WithRedis m => m (R.Reply ())
flushAll = getRedis >>= liftIO . R.flushAll

info :: WithRedis m => m R.RedisInfo
info = getRedis >>= liftIO . R.info

set :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply ())
set key val = do r <- getRedis
                 liftIO $ R.set r key val

setNx :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
setNx key val = do r <- getRedis
                   liftIO $ R.setNx r key val

setEx :: (WithRedis m, BS s1, BS s2) => s1 -> Int -> s2 -> m (R.Reply ())
setEx key seconds val = do r <- getRedis
                           liftIO $ R.setEx r key seconds val

mSet :: (WithRedis m, BS s1, BS s2) => [(s1, s2)] -> m (R.Reply ())
mSet ks = getRedis >>= liftIO . flip R.mSet ks

mSetNx :: (WithRedis m, BS s1, BS s2) => [(s1, s2)] -> m (R.Reply Int)
mSetNx ks = getRedis >>= liftIO . flip R.mSetNx ks

get :: (WithRedis m, BS s1, BS s2) => s1 -> m (R.Reply s2)
get key = getRedis >>= liftIO . flip R.get key

getSet :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> s2 -> m (R.Reply s3)
getSet key val = do r <- getRedis
                    liftIO $ R.getSet r key val

mGet :: (WithRedis m, BS s1, BS s2) => [s1] -> m (R.Reply s2)
mGet keys = getRedis >>= liftIO . flip R.mGet keys

incr :: (WithRedis m, BS s) => s -> m (R.Reply Int)
incr key = getRedis >>= liftIO . flip R.incr key

incrBy :: (WithRedis m, BS s) => s -> Int -> m (R.Reply Int)
incrBy key n = do r <- getRedis
                  liftIO $ R.incrBy r key n

decr :: (WithRedis m, BS s) => s -> m (R.Reply Int)
decr key = getRedis >>= liftIO . flip R.decr key

decrBy :: (WithRedis m, BS s) => s -> Int -> m (R.Reply Int)
decrBy key n = do r <- getRedis
                  liftIO $ R.decrBy r key n

append :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
append key str = do r <- getRedis
                    liftIO $ R.append r key str

substr :: (WithRedis m, BS s1, BS s2) => s1 -> (Int, Int) -> m (R.Reply s2)
substr key range = do r <- getRedis
                      liftIO $ R.substr r key range

getrange :: (WithRedis m, BS s1, BS s2) => s1 -> (Int, Int) -> m (R.Reply s2)
getrange key range = do r <- getRedis
                        liftIO $ R.getrange r key range

setrange :: (WithRedis m, BS s1, BS s2) => s1 -> Int -> s2 -> m (R.Reply Int)
setrange key offset val = do r <- getRedis
                             liftIO $ R.setrange r key offset val

getbit :: (WithRedis m, BS s) => s -> Int -> m (R.Reply Int)
getbit key offset = do r <- getRedis
                       liftIO $ R.getbit r key offset

setbit :: (WithRedis m, BS s) => s -> Int -> Int -> m (R.Reply Int)
setbit key offset bit = do r <- getRedis
                           liftIO $ R.setbit r key offset bit

strlen :: (WithRedis m, BS s) => s -> m (R.Reply Int)
strlen key = getRedis >>= liftIO . flip R.strlen key

rpush :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
rpush key val = do r <- getRedis
                   liftIO $ R.rpush r key val

lpush :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
lpush key val = do r <- getRedis
                   liftIO $ R.lpush r key val

rpushx :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
rpushx key val = do r <- getRedis
                    liftIO $ R.rpushx r key val

lpushx :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
lpushx key val = do r <- getRedis
                    liftIO $ R.lpushx r key val

linsert :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> R.LInsertDirection -> s2 -> s3 -> m (R.Reply Int)
linsert key direction anchor value = do r <- getRedis
                                        liftIO $ R.linsert r key direction anchor value

llen :: (WithRedis m, BS s) => s -> m (R.Reply Int)
llen key = getRedis >>= liftIO . flip R.llen key

lrange :: (WithRedis m, BS s1, BS s2) => s1 -> (Int, Int) -> m (R.Reply s2)
lrange key limit = do r <- getRedis
                      liftIO $ R.lrange r key limit

ltrim :: (WithRedis m, BS s) => s -> (Int, Int) -> m (R.Reply ())
ltrim key limit = do r <- getRedis
                     liftIO $ R.ltrim r key limit

lindex :: (WithRedis m, BS s1, BS s2) => s1 -> Int -> m (R.Reply s2)
lindex key index = do r <- getRedis
                      liftIO $ R.lindex r key index

lset :: (WithRedis m, BS s1, BS s2) => s1 -> Int -> s2 -> m (R.Reply ())
lset key index val = do r <- getRedis
                        liftIO $ R.lset r key index val

lrem :: (WithRedis m, BS s1, BS s2) => s1 -> Int -> s2 -> m (R.Reply Int)
lrem key count value = do r <- getRedis
                          liftIO $ R.lrem r key count value

lpop :: (WithRedis m, BS s1, BS s2) => s1 -> m (R.Reply s2)
lpop key = getRedis >>= liftIO . flip R.lpop key

rpop :: (WithRedis m, BS s1, BS s2) => s1 -> m (R.Reply s2)
rpop key = getRedis >>= liftIO . flip R.rpop key

rpoplpush :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> s2 -> m (R.Reply s3)
rpoplpush src dst = do r <- getRedis
                       liftIO $ R.rpoplpush r src dst

blpop :: (WithRedis m, BS s1, BS s2) => [s1] -> Int -> m (Maybe (s1, s2))
blpop keys timeout = do r <- getRedis
                        liftIO $ R.blpop r keys timeout

brpop :: (WithRedis m, BS s1, BS s2) => [s1] -> Int -> m (Maybe (s1, s2))
brpop keys timeout = do r <- getRedis
                        liftIO $ R.brpop r keys timeout

brpoplpush :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> s2 -> Int -> m (Maybe (Maybe s3))
brpoplpush src dst timeout = do r <- getRedis
                                liftIO $ R.brpoplpush r src dst timeout

sadd :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
sadd key val = do r <- getRedis
                  liftIO $ R.sadd r key val

srem :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
srem key val = do r <- getRedis
                  liftIO $ R.srem r key val

spop :: (WithRedis m, BS s1, BS s2) => s1 -> m (R.Reply s2)
spop key = getRedis >>= liftIO . flip R.spop key

smove :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> s2 -> s3 -> m (R.Reply Int)
smove src dst member = do r <- getRedis
                          liftIO $ R.smove r src dst member

scard :: (WithRedis m, BS s) => s -> m (R.Reply Int)
scard key = getRedis >>= liftIO . flip R.scard key

sismember :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
sismember key val = do r <- getRedis
                       liftIO $ R.sismember r key val

smembers :: (WithRedis m, BS s1, BS s2) => s1 -> m (R.Reply s2)
smembers key = getRedis >>= liftIO . flip R.smembers key

srandmember :: (WithRedis m, BS s1, BS s2) => s1 -> m (R.Reply s2)
srandmember key = getRedis >>= liftIO . flip R.srandmember key

sinter :: (WithRedis m, BS s1, BS s2) => [s1] -> m (R.Reply s2)
sinter key = getRedis >>= liftIO . flip R.sinter key

sinterStore :: (WithRedis m, BS s1, BS s2) => s1 -> [s2] -> m (R.Reply ())
sinterStore dst keys = do r <- getRedis
                          liftIO $ R.sinterStore r dst keys

sunion :: (WithRedis m, BS s1, BS s2) => [s1] -> m (R.Reply s2)
sunion keys = getRedis >>= liftIO . flip R.sunion keys

sunionStore :: (WithRedis m, BS s1, BS s2) => s1 -> [s2] -> m (R.Reply ())
sunionStore dst keys = do r <- getRedis
                          liftIO $ R.sunionStore r dst keys

sdiff :: (WithRedis m, BS s1, BS s2) => [s1] -> m (R.Reply s2)
sdiff keys = getRedis >>= liftIO . flip R.sdiff keys

sdiffStore :: (WithRedis m, BS s1, BS s2) => s1 -> [s2] -> m (R.Reply ())
sdiffStore dst keys = do r <- getRedis
                         liftIO $ R.sdiffStore r dst keys

zadd :: (WithRedis m, BS s1, BS s2) => s1 -> Double -> s2 -> m (R.Reply Int)
zadd key score member = do r <- getRedis
                           liftIO $ R.zadd r key score member

zrem :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
zrem key member = do r <- getRedis
                     liftIO $ R.zrem r key member

zincrBy :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> Double -> s2 -> m (R.Reply s3)
zincrBy key score member = do r <- getRedis
                              liftIO $ R.zincrBy r key score member

zrange :: (WithRedis m, BS s1, BS s2) => s1 -> (Int, Int) -> Bool -> m (R.Reply s2)
zrange key limit withscores = do r <- getRedis
                                 liftIO $ R.zrange r key limit withscores

zrevrange :: (WithRedis m, BS s1, BS s2) => s1 -> (Int, Int) -> Bool -> m (R.Reply s2)
zrevrange key limit withscores = do r <- getRedis
                                    liftIO $ R.zrevrange r key limit withscores

zrangebyscore :: (WithRedis m, IsInterval i Double, BS s1, BS s2) => s1 -> i -> Maybe (Int, Int) -> Bool -> m (R.Reply s2)
zrangebyscore key i limit withscores = do r <- getRedis
                                          liftIO $ R.zrangebyscore r key i limit withscores

zrevrangebyscore :: (WithRedis m, IsInterval i Double, BS s1, BS s2) => s1 -> i -> Maybe (Int, Int) -> Bool -> m (R.Reply s2)
zrevrangebyscore key i limit withscores = do r <- getRedis
                                             liftIO $ R.zrevrangebyscore r key i limit withscores

zcount :: (WithRedis m, IsInterval i Double, BS s) => s -> i -> m (R.Reply Int)
zcount key limit = do r <- getRedis
                      liftIO $ R.zcount r key limit

zremrangebyscore :: (WithRedis m, BS s) => s -> (Double, Double) -> m (R.Reply Int)
zremrangebyscore key limit = do r <- getRedis
                                liftIO $ R.zremrangebyscore r key limit

zcard :: (WithRedis m, BS s) => s -> m (R.Reply Int)
zcard key = getRedis >>= liftIO . flip R.zcard key

zscore :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> s2 -> m (R.Reply s3)
zscore key member = do r <- getRedis
                       liftIO $ R.zscore r key member

zrank :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
zrank key member = do r <- getRedis
                      liftIO $ R.zrank r key member

zrevrank :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
zrevrank key member = do r <- getRedis
                         liftIO $ R.zrevrank r key member

zremrangebyrank :: (WithRedis m, BS s) => s -> (Int, Int) -> m (R.Reply Int)
zremrangebyrank key limit = do r <- getRedis
                               liftIO $ R.zremrangebyrank r key limit

zunionStore :: (WithRedis m, BS s1, BS s2) => s1 -> [s2] -> [Double] -> R.Aggregate -> m (R.Reply Int)
zunionStore dst src weights aggregate = do r <- getRedis
                                           liftIO $ R.zunionStore r dst src weights aggregate

{-# DEPRECATED zunion "ZUNION command was renamed to ZUNIONSTORE" #-}
zunion :: (WithRedis m, BS s1, BS s2) => s1 -> [s2] -> [Double] -> R.Aggregate -> m (R.Reply Int)
zunion = zunionStore

zinterStore :: (WithRedis m, BS s1, BS s2) => s1 -> [s2] -> [Double] -> R.Aggregate -> m (R.Reply Int)
zinterStore dst src weights aggregate = do r <- getRedis
                                           liftIO $ R.zinterStore r dst src weights aggregate

{-# DEPRECATED zinter "ZINTER command was renamed to ZINTERSTORE" #-}
zinter :: (WithRedis m, BS s1, BS s2) => s1 -> [s2] -> [Double] -> R.Aggregate -> m (R.Reply Int)
zinter = zinterStore

hset :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> s2 -> s3 -> m (R.Reply Int)
hset key field value = do r <- getRedis
                          liftIO $ R.hset r key field value

hget :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> s2 -> m (R.Reply s3)
hget key field = do r <- getRedis
                    liftIO $ R.hget r key field

hdel :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
hdel key field = do r <- getRedis
                    liftIO $ R.hdel r key field

hmset :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> [(s2, s3)] -> m (R.Reply ())
hmset key fields = do r <- getRedis
                      liftIO $ R.hmset r key fields

hmget :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> [s2] -> m (R.Reply s3)
hmget key fields = do r <- getRedis
                      liftIO $ R.hmget r key fields

hincrby :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> Int -> m (R.Reply Int)
hincrby key field n = do r <- getRedis
                         liftIO $ R.hincrby r key field n

hexists :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
hexists key field = do r <- getRedis
                       liftIO $ R.hexists r key field

hlen :: (WithRedis m, BS s) => s -> m (R.Reply Int)
hlen key = do r <- getRedis
              liftIO $ R.hlen r key

hkeys :: (WithRedis m, BS s1, BS s2) => s1 -> m (R.Reply s2)
hkeys key = do r <- getRedis
               liftIO $ R.hkeys r key

hvals :: (WithRedis m, BS s1, BS s2) => s1 -> m (R.Reply s2)
hvals key = do r <- getRedis
               liftIO $ R.hvals r key

hgetall :: (WithRedis m, BS s1, BS s2) => s1 -> m (R.Reply s2)
hgetall key = do r <- getRedis
                 liftIO $ R.hgetall r key

sort :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> R.SortOptions s2 -> m (R.Reply s3)
sort key opt = do r <- getRedis
                  liftIO $ R.sort r key opt

listRelated :: (WithRedis m, BS s1, BS s2, BS s3) => s1 -> s2 -> (Int, Int) -> m (R.Reply s3)
listRelated related key l = do r <- getRedis
                               liftIO $ R.listRelated r related key l

subscribed :: (WithRedis m) => m Int
subscribed = getRedis >>= liftIO . R.subscribed

subscribe :: (WithRedis m, BS s1, BS s2) => [s1] -> m [R.Message s2]
subscribe classes = getRedis >>= liftIO . flip R.subscribe classes

unsubscribe :: (WithRedis m, BS s1, BS s2) => [s1] -> m [R.Message s2]
unsubscribe classes = getRedis >>= liftIO . flip R.unsubscribe classes

psubscribe :: (WithRedis m, BS s1, BS s2) => [s1] -> m [R.Message s2]
psubscribe classes = getRedis >>= liftIO . flip R.psubscribe classes

punsubscribe :: (WithRedis m, BS s1, BS s2) => [s1] -> m [R.Message s2]
punsubscribe classes = getRedis >>= liftIO . flip R.punsubscribe classes

publish :: (WithRedis m, BS s1, BS s2) => s1 -> s2 -> m (R.Reply Int)
publish klass msg = do r <- getRedis
                       liftIO $ R.publish r klass msg

listen :: (WithRedis m, BS s) => Int -> m (Maybe (R.Message s))
listen timeout = getRedis >>= liftIO . flip R.listen timeout

save :: WithRedis m => m (R.Reply ())
save = getRedis >>= liftIO . R.save

bgsave :: WithRedis m => m (R.Reply ())
bgsave = getRedis >>= liftIO . R.bgsave

lastsave :: WithRedis m => m (R.Reply Int)
lastsave = getRedis >>= liftIO . R.lastsave

bgrewriteaof :: WithRedis m => m (R.Reply ())
bgrewriteaof = getRedis >>= liftIO . R.bgrewriteaof
