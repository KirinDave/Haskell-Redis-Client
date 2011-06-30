{-# LANGUAGE PackageImports #-}
module Test.ZSetCommands where

import Data.List
import Data.Map (Map(..), fromList, toList, unionWith, intersectionWith)
import qualified Data.Map as M
import Test.Setup

tests = TList [TLabel "zrange, zrevrange and scard" test_zrange_zrevrange_zcard,
               TLabel "zadd and zrem" test_zadd_zrem,
               TLabel "zscore" test_zscore,
               TLabel "zincrBy" test_zincrBy,
               TLabel "zrangebyscore" test_zrangebyscore,
               TLabel "zrevrangebyscore" test_zrevrangebyscore,
               TLabel "zcount" test_zcount,
               TLabel "zremrangebyscore" test_zremrangebyscore,
               TLabel "zrank and zrevrank" test_zrank_zrevrank,
               TLabel "zremrangebyrank" test_zremrangebyrank,
               TLabel "zunionStore" test_zunionStore,
               TLabel "zinterStore" test_zinterStore]

asZSet :: Reply String -> IO [(String, Double)]
asZSet r = fromRMultiBulk r >>= return . build . map fromJust . fromJust
    where build (a:b:z) = (a, read b) : build z
          build (a:[]) = error "unpaired element"
          build [] = []

zsort :: [(String, Double)] -> [(String, Double)]
zsort = sortBy (\(_, a) (_, b) -> compare a b)

test_zrange_zrevrange_zcard = TCase $ testRedis $
    let expected = zsort $ zip ["1", "2", "3", "4", "5"] (reverse [1.0, 2.0, 3.0, 4.0, 5.0])
    in do r <- ask
          addZSet
          liftIO $ do zrange r "zset" takeAll True >>= asZSet >>= assertEqual "" expected
                      zrevrange r "zset" takeAll True >>= asZSet >>= assertEqual "" (reverse expected)
                      zcard r "zset" >>= fromRInt >>= assertEqual "" (length expected)
                      zcard r "no-such-key" >>= fromRInt >>= assertEqual "" 0
                      zrange r "no-such-key" takeAll True >>= asZSet >>= assertEqual "" []
                      zrevrange r "no-such-key" takeAll True >>= asZSet >>= assertEqual "" []
                      zrange r "zset" (1,2) True >>= asZSet >>= assertEqual "" (take 2 $ drop 1 $ expected)
                      zrevrange r "zset" (1,2) True >>= asZSet >>= assertEqual "" (take 2 $ drop 1 $ reverse expected)
                      zrange r "zset" (1,2) False >>= fromRMultiBulk >>= return . map fromJust . fromJust >>= assertEqual "" (map fst $ take 2 $ drop 1 $ expected)
                      zrevrange r "zset" (1,2) False >>= fromRMultiBulk >>= return . map fromJust . fromJust >>= assertEqual "" (map fst $ take 2 $ drop 1 $ reverse expected)

test_zadd_zrem = TCase $ testRedis $
    do r <- ask
       addZSet
       liftIO $ do zadd r "zset" 0.5 "6" >>= fromRInt >>= assertEqual "" 1
                   zadd r "zset" 0.6 "1" >>= fromRInt >>= assertEqual "element is allready in set" 0
                   z <- zrange r "zset" takeAll True >>= asZSet
                   assertEqual "" ("6", 0.5) $ head z
                   assertEqual "" ("1", 0.6) $ head $ tail z
                   zrem r "zset" "6" >>= fromRInt >>= assertEqual "" 1
                   zrem r "zset" "6" >>= fromRInt >>= assertEqual "" 0
                   z <- zrange r "zset" takeAll True >>= asZSet
                   assertEqual "" ("1", 0.6) $ head z

test_zscore = TCase $ testRedis $
    let lookupBy _ _ [] = Nothing
        lookupBy f a (l:ls) = if f l == a
                              then Just l
                              else lookupBy f a ls
    in do r <- ask
          addZSet
          liftIO $ do z <- zrange r "zset" takeAll True >>= asZSet
                      let expected = snd `fmap` lookupBy fst "1" z
                      score <- zscore r "zset" "1" >>= fromRBulk :: IO (Maybe Double)
                      assertEqual "" expected score

test_zincrBy = TCase $ testRedis $
    do r <- ask
       addZSet
       liftIO $ do zincrBy r "zset" 0.5 "5" >>= fromRBulk >>= assertEqual "" (Just (1.5 :: Double))
                   score <- zscore r "zset" "5"  >>= fromRBulk
                   assertEqual "" (Just (1.5 :: Double)) score
                   zincrBy r "zset" (-0.5) "5" >>= fromRBulk >>= assertEqual "" (Just (1.0 :: Double))
                   score <- zscore r "zset" "5"  >>= fromRBulk
                   assertEqual "" (Just (1.0 :: Double)) score

test_zrangebyscore = TCase $ testRedis $
    do r <- ask
       addZSet
       liftIO $ do z <- zrange r "zset" takeAll True >>= asZSet
                   z' <- zrangebyscore r "zset" [1, 4] (Just (1, 2)) True >>= asZSet
                   let expected = take 2 $ drop 1 $ filter ((\x -> x >= 1 && x <= 4) . snd) z
                   assertEqual "" expected z'
                   z' <- zrangebyscore r "zset" (1 :: Double, 4 :: Double) (Just (1, 2)) True >>= asZSet
                   let expected = take 2 $ drop 1 $ filter ((\x -> x > 1 && x < 4) . snd) z
                   assertEqual "" expected z'

test_zrevrangebyscore = TCase $ testRedis $
    do r <- ask
       addZSet
       liftIO $ do z <- zrange r "zset" takeAll True >>= asZSet
                   z' <- zrevrangebyscore r "zset" [4, 1] (Just (1, 2)) True >>= asZSet
                   let expected = take 2 $ drop 1 $ reverse $ filter ((\x -> x >= 1 && x <= 4) . snd) z
                   assertEqual "[1, 4]" expected z'
                   z' <- zrevrangebyscore r "zset" (4 :: Double, 1 :: Double) (Just (1, 2)) True >>= asZSet
                   let expected = take 2 $ drop 1 $ reverse $ filter ((\x -> x > 1 && x < 4) . snd) z
                   assertEqual "(1, 4)" expected z'

test_zcount = TCase $ testRedis $
    do r <- ask
       addZSet
       liftIO $ do z <- zrange r "zset" takeAll True >>= asZSet
                   z' <- zcount r "zset" [1, 4] >>= fromRInt
                   let expected = filter ((\x -> x >= 1 && x <= 4) . snd) z
                   assertEqual "" (length expected) z'
                   z' <- zcount r "zset" (1 :: Double, 4 :: Double) >>= fromRInt
                   let expected = filter ((\x -> x > 1 && x < 4) . snd) z
                   assertEqual "" (length expected) z'

test_zremrangebyscore = TCase $ testRedis $
    do r <- ask
       addZSet
       liftIO $ do z <- zrange r "zset" takeAll True >>= asZSet
                   let removed = filter ((\x -> x >= 1 && x <= 4) . snd) z
                       left = filter (not . (\x -> x >= 1 && x <= 4) . snd) z
                   zremrangebyscore r "zset" (1, 4) >>= fromRInt >>= assertEqual "" (length removed)
                   z' <- zrange r "zset" takeAll True >>= asZSet
                   assertEqual "" left z'

test_zrank_zrevrank = TCase $ testRedis $
    let rank e = findIndex (== e)
    in do r <- ask
          addZSet
          liftIO $ do z <- zrange r "zset" takeAll False >>= fromRMultiBulk >>= return . fromJust
                      c <- zrank r "zset" "3" >>= fromRInt
                      assertEqual "" (fromJust $ rank (Just "3") z) c
                      c <- zrevrank r "zset" "3" >>= fromRInt
                      assertEqual "" (fromJust $ rank (Just "3") $ reverse z) c

                      {- The next one doesn't works because of zrank returning
                         "RBulk Nil" if requested element is not a
                         member of set
                         zrank r "zset" "6" >>= fromRInt
                       -}

test_zremrangebyrank = TCase $ testRedis $
    do r <- ask
       addZSet
       liftIO $ do z <- zrange r "zset" takeAll False >>= fromRMultiBulk >>= return . fromJust :: IO [Maybe String]
                   let expected = (take 1 z) ++ (drop 4 $ z) -- cut elements from 1 to 3
                   zremrangebyrank r "zset" (1, 3) >>= fromRInt >>= assertEqual "" (length z - length expected)
                   z' <- zrange r "zset" takeAll False >>= fromRMultiBulk >>= return . fromJust :: IO [Maybe String]
                   assertEqual "" expected z'

test_zunionStore = TCase $ testRedis $
    let addZSet2 key m = ask >>= \r ->  liftIO $ mapM_ (uncurry (flip $ zadd r key)) $ toList m
        zmap2 = fromList $ zip ["1", "2", "6"] [1.0, 2.0, 3.0] :: Map String Double
    in do r <- ask
          addZSet
          addZSet2 "zset2" zmap2
          liftIO $ do z <- zrange r "zset" takeAll True >>= asZSet
                      let zmap = fromList z :: Map String Double
                          expected = zsort $ toList $ unionWith (+) zmap zmap2
                      res <- zunionStore r "zset3" ["zset", "zset2"] [1, 1] SUM >>= fromRInt
                      assertEqual "" (length expected) res
                      z' <- zrange r "zset3" takeAll True >>= asZSet
                      assertEqual "" expected z'

                      let zmap = fromList z :: Map String Double
                          -- in Redis weights is applied to maps before aggregation
                          expected = zsort $ toList $ unionWith (+) zmap $ M.map (* 0.5) zmap2
                      res <- zunionStore r "zset3" ["zset", "zset2"] [1, 0.5] SUM >>= fromRInt
                      assertEqual "" (length expected) res
                      z' <- zrange r "zset3" takeAll True >>= asZSet
                      assertEqual "" expected z'

test_zinterStore = TCase $ testRedis $
    let addZSet2 key m = ask >>= \r -> liftIO $ mapM_ (uncurry (flip $ zadd r key)) $ toList m
        zmap2 = fromList $ zip ["1", "2", "6"] [1.0, 2.0, 3.0] :: Map String Double
    in do r <- ask
          addZSet
          addZSet2 "zset2" zmap2
          liftIO $ do z <- zrange r "zset" takeAll True >>= asZSet
                      let zmap = fromList z :: Map String Double
                          expected = zsort $ toList $ intersectionWith (+) zmap zmap2
                      res <- zinterStore r "zset3" ["zset", "zset2"] [1, 1] SUM >>= fromRInt
                      assertEqual "" (length expected) res
                      z' <- zrange r "zset3" takeAll True >>= asZSet
                      assertEqual "" expected z'

                      let zmap = fromList z :: Map String Double
                          -- in Redis weights is applied to maps before aggregation
                          expected = zsort $ toList $ intersectionWith (+) zmap $ M.map (* 0.5) zmap2
                      res <- zinterStore r "zset3" ["zset", "zset2"] [1, 0.5] SUM >>= fromRInt
                      assertEqual "" (length expected) res
                      z' <- zrange r "zset3" takeAll True >>= asZSet
                      assertEqual "" expected z'
