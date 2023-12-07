from enum import EnumMeta
from sys import maxsize
from turtle import distance
from numpy import inf
import numpy as np
import math
import pandas as pd
import heapq
from queue import Queue

""" 斐波那契数列递归 """


def fibonacci_recursive(n):
    if n == 1 or n == 2:
        return n
    return fibonacci_recursive(n - 1) + fibonacci_recursive(n - 2)


""" 斐波那契数列迭代,List数据结构 """


def fibonacci_iterate(n):
    if n == 1 or n == 2:
        return n
    fi = [0, 1, 2]

    for i in range(3, n + 1):
        fi.append(fi[i - 2] + fi[i - 1])
    return fi[n]


""" 斐波那契数列迭代,numpy数组 """


def fibonacci_iterate(n):
    if n == 1 or n == 2:
        return n
    fi = np.array([0, 1, 2])

    for i in range(3, n + 1):
        fi = np.append(fi, fi[i - 2] + fi[i - 1])
    return fi[n]


""" 动态规划 """
""" 
7 1 2 3 4
3 8 5 6 7
8 1 0 1 2
2 7 4 4 5
4 5 2 6 5 
d(i,j)=max(d(i,j-1),d(i-1,j))
"""


def get_max_sum_recursive(li, row, col):
    tmp1 = 0
    tmp2 = 0
    if row == 0 and col == 0:
        return li[row][col]
    if row > 0:
        tmp1 = get_max_sum_recursive(li, row - 1, col)
    if col > 0:
        tmp2 = get_max_sum_recursive(li, row, col - 1)
    maxlist[row][col] = max(tmp1, tmp2) + li[row][col]
    return maxlist[row][col]


# print(get_max_sum_recursive(li, 1, 1))

li = [
    [7, 1, 2, 3, 4],
    [3, 8, 5, 6, 7],
    [8, 1, 0, 1, 2],
    [2, 7, 4, 4, 5],
    [4, 5, 2, 6, 5],
]
maxlist = [[0] * 5 for i in range(5)]


def get_max_sum_iterate(li, row, col):
    maxlist[0][0] = li[0][0]
    if row == 0 and col == 0:
        return li[row][col]
    if row > 0 or col > 0:
        for i in range(0, row + 1):
            for j in range(0, col + 1):
                if i > 0 and j > 0:
                    maxlist[i][j] = max(maxlist[i - 1][j], maxlist[i][j - 1]) + li[i][j]
                elif j == 0:
                    maxlist[i][j] = maxlist[i - 1][j] + li[i][j]
                else:
                    maxlist[i][j] = maxlist[i][j - 1] + li[i][j]

    return maxlist[row][col]


st = [7, 8, 1, 3, 4, 2, 7, 1]
""" pi是最低价格 """


def get_bestreturn(st, n):
    if n == 0:
        return 0
    if n == 1:
        return 0
    dp = [0 for i in range(n)]
    res = 0
    dp[0] = st[0]
    for j in range(1, n):
        if st[j] < dp[j - 1]:
            dp[j] = st[j]
        else:
            dp[j] = dp[j - 1]
        res = max(st[j] - dp[j], st[j - 1] - dp[j - 1])
    return res


""" 13=4+9 """
""" f[15]=4+9+1+1  4
f[14]=4+9+1 3
f[13]=4+9 2 
f[12]=4+4+4 3
f[i]= f[i-j*j]+1
"""


def get_sum_square(n):
    if n == 0:
        return 0
    f = [inf for h in range(0, n + 1)]
    f[0] = 0
    for i in range(1, n + 1):
        for j in range(1, i + 1):
            if j * j > i:
                break
            if f[i - j * j] + 1 < f[i]:
                f[i] = f[i - j * j] + 1
        print("f[%s] is : %s" % (i, f[i]))
    return f[n]


""" [3, 5, 7] 
27 4*5+7 5
26 3*7+5 4
25 5*5 5
f[i]是最少的组合成i的硬币组合
f[i] = f[i - a[j]] + 1
"""
a = [3, 5, 7]


def get_coins_min(a, n):
    f = [inf for h in range(0, n + 1)]
    f[0] = 0
    for i in range(1, n + 1):
        for j in range(0, len(a)):
            if i < a[j]:
                continue
            if f[i - a[j]] + 1 < f[i]:
                f[i] = f[i - a[j]] + 1
        print("f[%s] is : %s" % (i, f[i]))
    y = lambda x: -1 if x == inf else x
    return y(f[n])


# 二叉树类
class BTree(object):

    # 初始化
    def __init__(self, data=None, left=None, right=None):
        self.data = data  # 数据域
        self.left = left  # 左子树
        self.right = right  # 右子树

    # 前序遍历
    def preorder(self):

        if self.data is not None:
            print(self.data, end=" ")
        if self.left is not None:
            self.left.preorder()
        if self.right is not None:
            self.right.preorder()

    # 中序遍历
    def inorder(self):

        if self.left is not None:
            self.left.inorder()
        if self.data is not None:
            print(self.data, end=" ")
        if self.right is not None:
            self.right.inorder()

    # 后序遍历
    def postorder(self):

        if self.left is not None:
            self.left.postorder()
        if self.right is not None:
            self.right.postorder()
        if self.data is not None:
            print(self.data, end=" ")

    # 二叉树的高度
    def height(self):
        # 空的树高度为0, 只有root节点的树高度为1
        if self.data is None:
            return 0
        elif self.left is None and self.right is None:
            return 1
        elif self.left is None and self.right is not None:
            return 1 + self.right.height()
        elif self.left is not None and self.right is None:
            return 1 + self.left.height()
        else:
            return 1 + max(self.left.height(), self.right.height())

    # 二叉树的叶子节点
    def leaves(self):

        if self.data is None:
            return None
        elif self.left is None and self.right is None:
            print(self.data, end=" ")
        elif self.left is None and self.right is not None:
            self.right.leaves()
        elif self.right is None and self.left is not None:
            self.left.leaves()
        else:
            self.left.leaves()
            self.right.leaves()


# 构造二叉树, BOTTOM-UP METHOD
left_tree = BTree(3)
left_tree.left = BTree(2)
left_tree.right = BTree(4)
right_tree = BTree(7)
right_tree.left = BTree(6)
right_tree.right = BTree(8)
tree = BTree(5)
tree.left = left_tree
tree.right = right_tree

# print(tree.leaves())


# distances = [3, 2, 4, 5, 2, 10, 12]
# prices = [7, 6, 5, 7, 3, 2, 8, 9]

"""
dp[0] = 3*7
dp[1] = 3*7 + 10*6
dp[2] = 3*7 + 10*6 + 10*5
dp[3] = 3*7 + 10*6 + 10*5 + 20*3
dp[4] = 3*7 + 10*6 + 10*5 + 20*3 + 30*2
dp[5] = 3*7 + 10*6 + 10*5 + 20*3 + 30*2 + 10*2
dp[6] + 3*7 + 10*6 + 10*5 + 20*3 + 30*2 + 10*2 + 10*2 + 2*9

distances = [3, 2, 4, 5]
prices = [7, 6, 5, 7]
dp[i][j] 第i个站点加j升油
  0  1  2  3
  3  2  4  5
0 0  0  0  0
1 0  0  0  0
2 0  0  0  0
3 1  1  1  1
4 1  1  1  1
5 1  1  1  1
..
"""


# def get_min_cost(distances, prices):
#     # dp[i]到第i站的最小花费
#     # rest[i]到第i站的剩余油量
#     dp = [0 for h in range(0, len(distances) + 1)]
#     cost = [0 for h in range(0, len(distances) + 1)]
#     dp[0] = 0
#     avail = 0
#     min_price = 0
#     for i in range(0, len(distances) + 1):
#         for j in range(i + 1, len(distances) + 1):
#             if i == 0 and j == 1:
#                 if prices[i] >= prices[j]:
#                     min_price = prices[i]
#                 cost[i] = distances[i] * prices[i]
#                 avail = distances[i]
#             else:
#                 # 求范围内最小价格
#                 if prices[i] >= prices[j]:
#                     min_price = prices[j]
#                     cost[j] = distances[i] * prices[i]
#                     avail = distances[i]
#                     break
#                 else:
#                     min_price = prices[i]
#                     if distances[j] + distances[i] >= 50:
#                         dp[i] = 50 * min_price
#                         dp[j] = dp[i] + (50 - distances[j]) * prices[j]

#             print("current i is : %s" % (i))
#             print(min_price)
#             if distances[i] == 50:
#                 dp[i] = distances[i] * prices[i]
#                 rest[i] = 50
#             else:
#                 if prices[j] >= min_price and sum(distances[i:j]) < 50:
#                     continue
#                 else:
#                     rest[i] = sum(distances[i:j])
#                     dp[i] = (sum(distances[i:j]) - rest[i]) * min_price
#             i = j
#         if i == len(distances) and rest[i] == distances[i]:
#             return dp[len(distances)]


""" 
dp[0] = 0
dp[1] = 3*7
dp[2] = dp[1] + 10*6
dp[3] = dp[2] + 10*5
dp[4] = min(dp[3] + 10*7, dp[2] + 10*5 + 10*5)
dp[5] = min(dp[4] + 2*3, dp[3] + 10*5 + 10*5 + 2*3)
dp[6] = min(dp[5] + 3*2, dp[4] + 2*3 + 3*2)
dp[7] = min(dp[6] + 3*9, dp[5] + 3*2 + 3*2)

dp[i][50-rest[i]] 第i个站点加油 50-rest[i]的最小花费
dp[0][50-rest[0]] 第0个站点加油 50-rest[0]的最小花费
distances[0] <= 50-rest[0] <= 50
0 <= 50-rest[i] <= distances[i] and 0 < i <= len(distances)
dp[i][50-rest[i]] = min(dp[i-1][50-rest[i-1]], dp[i][50-rest[i]])

3*7 + 10*6 + 10*5
21 + 60 + 50 + 50 +6 + 6 + 6
131 + 
"""

# n个加油站，每个站标识到下站的路程
d = [3, 10, 10, 10, 2, 3, 3]
p = [7, 6,  5,  7,  3, 2, 9]
def get_min_cost(d, p):
    # dp[i][j]到第i站剩余油量j的最小花费
    # 到n站时剩余油量为0，即r=0
    # r[i] = j[i] + r[i - 1] - d[i - 1]
    w = len(d)
    t = 50
    dp = [[inf] * (t + 1) for h in range(0, w)]
    r = [0 for k in range(0, t + 1)]
    # 初始化数组
    # 第0个站点剩余0升油料
    r[0] = 0
    # 第0个站点剩0升油最小花费为0
    dp[0][0] = 0
    for j in range(0, t + 1):
        # 第0个站点加油 d[0] -> 50 循环遍历
        if j >= d[0]:
            r[0] = j
            dp[0][r[0]] = j * p[0]
        else:
            r[0] = j
            dp[0][r[0]] = inf
        # 第i个站点加油 0 -> 50,同时邮箱剩余油量要大于下一段路程
        for i in range(1, w):
            for h in range(t - r[i - 1] + d[i - 1] + 1, -1, -1):
                # 剩余油料                
                r[i] = r[i - 1] + h - d[i - 1]
                if r[i] <= t and r[i] >= d[i]:
                    dp[i][r[i]] = min((dp[i - 1][r[i - 1]] + h * p[i]), dp[i][r[i]])
    tl = pd.DataFrame(dp)
    print(tl)

    minv = min(dp[-1])
    return minv
    
# print(get_min_cost(d, p))

# jumping games
# f[i]代表是否可以跳到最后一步
# s = [3, 2, 4, 1, 2, 3]
# def is_jump(s):
#     if len(s) ==0 or len(s) ==1:
#         return True
#     for i in range(len(s), 0, -1):
#         if s[i] >= len(s) - i and 

