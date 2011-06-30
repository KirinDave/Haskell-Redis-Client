module Test.Lock where
import Test.Setup


tests = TList [TLabel "acquire and release" test_acquire_release,
               TLabel "acquire timeout" test_acquire_timeout]

test_acquire_release = TCase $ testRedis2 $
    do r1 <- ask
       r2 <- ask2
       liftIO $ do acquire r1 "lock" 1000 50 >>= assertBool ""
                   acquire r2 "lock" 1000 50 >>= assertEqual "" False
                   release r1 "lock"
                   acquire r2 "lock" 1000 50 >>= assertBool ""

test_acquire_timeout = TCase $ testRedis2 $
    let act v r = do acquire r "lock" 2000 50 >>= assertBool ""
                     putMVar v "done"
    in do v <- liftIO newEmptyMVar
          r1 <- ask
          r2 <- ask2
          liftIO $ do acquire r1 "lock" 1000 50 >>= assertBool ""
                      v2 <- later 500 $ do res <- tryTakeMVar v
                                           release r1 "lock"
                                           return res
                      act v r2
                      takeMVar v2 >>= assertEqual "" Nothing