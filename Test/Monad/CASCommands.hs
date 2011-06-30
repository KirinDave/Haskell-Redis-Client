module Test.Monad.CASCommands where

import Database.Redis.Monad
import Test.Setup hiding (exec, del, get, multi, run_cas, set)

tests = TList [TLabel "run_cas" test_run_cas,
               TLabel "run_cas that raised an exception" test_run_cas_exception]

run r = liftIO . runWithRedis r

test_run_cas = TCase $ testRedis2 $
    let act :: RedisM String
        act = do Just foo <- get "foo" >>= fromRBulk
                 multi
                 liftIO $ threadDelay 1000
                 set "foo" "bar" >>= noError
                 set "foo_" foo >>= noError
                 exec :: RedisM (Reply ())
                 return foo

    in do r1 <- ask
          r2 <- ask2
          addStr
          run r1 $ do foo <- run_cas ["foo", "bar"] act
                      foo_ <- get "foo_" >>= fromRBulk :: RedisM (Maybe String)
                      liftIO $ assertEqual "" (Just foo) foo_
                      get "foo" >>= fromRBulk >>= liftIO . assertEqual "" (Just "bar")

                      del "foo_"

          liftIO $ later 500 $ runWithRedis r2 $ set "foo" "baz"

          run r1 $ do run_cas ["foo", "bar"] act
                      foo_ <- get "foo_" >>= fromRBulk :: RedisM (Maybe String)
                      liftIO $ assertEqual "" Nothing foo_
                      get "foo" >>= fromRBulk >>= liftIO . assertEqual "" (Just "baz")

test_run_cas_exception = TCase $ testRedis $
    let act :: RedisM ()
        act = do multi
                 set "foo" "bar" >>= noError
                 set "bar" "foo" >>= noError
                 error "bang!"
                 exec :: RedisM (Reply ())
                 return ()
    in do r <- ask
          addStr
          foo <- run r $ (get "foo" :: RedisM (Reply String))
          liftIO $ assertRaises "" (ErrorCall undefined) $ runWithRedis r $ run_cas ["foo", "bar"] act
          run r $ get "foo" >>= liftIO . assertEqual "" foo
