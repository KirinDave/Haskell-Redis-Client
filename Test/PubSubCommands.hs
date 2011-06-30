module Test.PubSubCommands where

import Test.Setup

tests = TList [TLabel "subscribe and unsubscribe" test_subscribe_unsubscribe,
               TLabel "psubscribe and punsubscribe" test_psubscribe_punsubscribe,
               TLabel "publish and listen" test_publish_listen,
               TLabel "publish and listen for psubscribe" test_publish_listen_p]

test_subscribe_unsubscribe = TCase $ testRedis $
    do r <- ask
       liftIO $ do subscribed r >>= assertEqual "" 0
                   res <- subscribe r ["foo", "bar"]
                   assertEqual "" [MSubscribe "foo" 1, MSubscribe "bar" 2] res
                   subscribed r >>= assertEqual "" 2

                   res <- subscribe r ["foo", "baz"]
                   assertEqual "" [MSubscribe "foo" 2, MSubscribe "baz" 3] res
                   subscribed r >>= assertEqual "" 3

                   res <- unsubscribe r ["foo"]
                   assertEqual "" [MUnsubscribe "foo" 2] res
                   subscribed r >>= assertEqual "" 2

                   ping r >>= assertRError ""

                   res <- unsubscribe r ([] :: [String]) :: IO [Message String]
                   subscribed r >>= assertEqual "" 0

test_psubscribe_punsubscribe = TCase $ testRedis $
    do r <- ask
       liftIO $ do subscribed r >>= assertEqual "" 0
                   res <- psubscribe r ["foo*", "bar*"]
                   assertEqual "" [MPSubscribe "foo*" 1, MPSubscribe "bar*" 2] res
                   subscribed r >>= assertEqual "" 2

                   res <- psubscribe r ["foo*", "baz*"]
                   assertEqual "" [MPSubscribe "foo*" 2, MPSubscribe "baz*" 3] res
                   subscribed r >>= assertEqual "" 3

                   res <- punsubscribe r ["foo*"]
                   assertEqual "" [MPUnsubscribe "foo*" 2] res
                   subscribed r >>= assertEqual "" 2

                   ping r >>= assertRError ""

                   res <- punsubscribe r ([] :: [String]) :: IO [Message String]
                   subscribed r >>= assertEqual "" 0

test_publish_listen = TCase $ testRedis2 $
    do r1 <- ask
       r2 <- ask2
       liftIO $ do subscribed r1 >>= assertEqual "" 0
                   subscribe r1 ["foo"] :: IO [Message ()]
                   v <- later 500 $ publish r2 "foo" "bar" >>= fromRInt
                   msg <- listen r1 1000
                   assertEqual "" (Just $ MMessage "foo" "bar") msg
                   takeMVar v >>= assertEqual "" 1

                   (listen r1 500 :: IO (Maybe (Message String))) >>= assertEqual "" Nothing

test_publish_listen_p = TCase $ testRedis2 $
    do r1 <- ask
       r2 <- ask2
       liftIO $ do subscribed r1 >>= assertEqual "" 0
                   psubscribe r1 ["foo*", "f*"] :: IO [Message ()]
                   v <- later 500 $ publish r2 "fun" "funny" >>= fromRInt
                   msg <- listen r1 1000
                   assertEqual "" (Just $ MPMessage "f*" "fun" "funny") msg
                   takeMVar v >>= assertEqual "" 1
                   v <- later 500 $ publish r2 "foot" "football" >>= fromRInt
                   msg <- listen r1 1000
                   assertEqual "" (Just $ MPMessage "foo*" "foot" "football") msg
                   msg <- listen r1 1000
                   assertEqual "" (Just $ MPMessage "f*" "foot" "football") msg
                   takeMVar v >>= assertEqual "" 2
                   (listen r1 500 :: IO (Maybe (Message String))) >>= assertEqual "" Nothing
