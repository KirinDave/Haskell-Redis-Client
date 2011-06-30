module Test.Connection where

import Test.Setup


tests = TList [TLabel "connect" test_connect,
               TLabel "connection state" test_connection_state]

test_connect = TCase $ \opts -> do r <- connect (optHost opts) (optPort opts)
                                   connected <- isConnected r
                                   assertBool "Connection failed for some reason" connected
                                   disconnect r
                                   connected <- isConnected r
                                   assertBool "Disconnected but still in 'connected' state" $ not connected

test_connection_state = TCase $ \opts -> flip testRedis opts $
    do r <- ask
       liftIO $ do server <- getServer r
                   assertEqual "Wrong server in connection state" (optHost opts, optPort opts) server
                   db <- getDatabase r
                   assertEqual "Wrond database number in connection state" (optDatabase opts) db
