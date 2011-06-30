module Test.SortCommands where

import Data.Char (toUpper)
import Data.List hiding (sort)
import Data.ByteString.UTF8 (fromString)
import qualified Data.List as L
import System.Random
import Test.Setup

tests = TList [TLabel "sort asc and desc" test_sort_asc_desc,
               TLabel "sort alpha" test_sort_alpha,
               TLabel "sort_by" test_sort_by,
               TLabel "sort get_obj" test_sort_get,
               TLabel "sort store" test_sort_store,
               TLabel "listRelated" test_listRelated]

addUserList name l = do r <- ask
                        lift $ mapM_ (rpush r name) l

unsortedList = randomRIOs ((0 :: Int), 99) 10
unsortedAlphaList = randomRIOs ('a', 'z') 10
unsortedAlphaList2 = randomRIOs ('а', 'я') 10 -- non-ASCII string

test_sort_asc_desc = TCase $ testRedis $
    do r <- ask
       l <- liftIO unsortedList
       addUserList "l" l
       liftIO $ do l' <- sort r "l" sortDefaults >>= fromRMultiBulk >>= return . map fromJust . fromJust
                   assertEqual "" (L.sort l) l'
                   l' <- sort r "l" sortDefaults{desc = True} >>= fromRMultiBulk >>= return . map fromJust . fromJust
                   assertEqual "" (reverse $ L.sort l) l'

test_sort_alpha = TCase $ testRedis $
    do r <- ask
       l1 <- liftIO unsortedAlphaList
       l2 <- liftIO unsortedAlphaList2
       addUserList "l1" l1
       addUserList "l2" l2
       liftIO $ do l' <- sort r "l1" sortDefaults{alpha = True} >>= fromRMultiBulk >>= return . map fromJust . fromJust
                   assertEqual "" (L.sort l1) l'
                   l' <- sort r "l2" sortDefaults{alpha = True} >>= fromRMultiBulk >>= return . map fromJust . fromJust
                   assertEqual "" (L.sort l2) l'
                   l' <- sort r "l1" sortDefaults{desc = True, alpha = True} >>= fromRMultiBulk >>= return . map fromJust . fromJust
                   assertEqual "" (reverse $ L.sort l1) l'
                   l' <- sort r "l2" sortDefaults{desc = True, alpha = True} >>= fromRMultiBulk >>= return . map fromJust . fromJust
                   assertEqual "" (reverse $ L.sort l2) l'

test_sort_by = TCase $ testRedis $
    do r <- ask
       l <- liftIO unsortedList
       addUserList "l" l
       -- set bounch of kes foo_* for sort l in reverse order
       liftIO $ mapM_ (\x -> set r ("foo_" ++ show x) (-x)) [(0 :: Int)..99]

       liftIO $ do l' <- sort r "l" sortDefaults{sort_by = fromString "foo_*"} >>= fromRMultiBulk >>= return . map fromJust . fromJust
                   assertEqual "" (reverse $ L.sort l) l'
                   l' <- sort r "l" sortDefaults{sort_by = fromString "constant"} >>= fromRMultiBulk >>= return . map fromJust . fromJust
                   assertEqual "" l l'

test_sort_get = TCase $ testRedis $
    do r <- ask
       l <- liftIO unsortedAlphaList
       addUserList "l" l
       liftIO $ mapM_ (\x -> set r ("foo_" ++ [x]) (toUpper x) >> set r ("bar_" ++ [x]) 'x') ['a' .. 'z']

       liftIO $ do l' <- sort r "l" sortDefaults{alpha = True, get_obj = [fromString "foo_*", fromString "bar_*"]} >>= fromRMultiBulk >>= return . map fromJust . fromJust
                   assertEqual "" ((++ "x") $ intersperse 'x' $ map toUpper $ L.sort l) l'

test_sort_store = TCase $ testRedis $
    do r <- ask
       l <- liftIO unsortedAlphaList
       addUserList "l" l
       liftIO $ mapM_ (\x -> set r ("foo_" ++ [x]) (toUpper x) >> set r ("bar_" ++ [x]) 'x') ['a' .. 'z']
       liftIO $ do l' <- sort r "l" sortDefaults{alpha = True, get_obj = [fromString "foo_*"]} >>= fromRMultiBulk >>= return . map fromJust . fromJust :: IO [Char]
                   (sort r "l" sortDefaults{alpha = True, get_obj = [fromString "foo_*"], store = fromString "l2"} :: IO (Reply ())) >>= noError
                   l'' <- lrange r "l2" takeAll >>= fromRMultiBulk >>= return . map fromJust . fromJust
                   assertEqual "" l' l''

test_listRelated = TCase $ testRedis $
    do r <- ask
       addList
       liftIO $ mapM_ (\x -> set r ("foo_" ++ show x) (x^2)) [(1 :: Int)..3]
       liftIO $ do l <- lrange r "list" takeAll >>= fromRMultiBulk >>= return . map fromJust . fromJust :: IO [Int]
                   l' <- listRelated r "foo_*" "list" takeAll >>= fromRMultiBulk >>= return . map fromJust . fromJust
                   assertEqual  "" (map (^2) l) l'
