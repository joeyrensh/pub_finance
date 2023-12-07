from audioop import reverse


class listNode:
    def __init__(self, data=None, next=None):
        self.data = data
        self.next = None


def create_linkedlist_head(li):
    head = listNode(li[0])
    for element in li[1:]:
        node = listNode(element)
        node.next = head
        head = node
    return head

# 归并合并链表
def merge_sort(head):
    if not head or not head.next:
        return head
    # 分割链表
    head, newhead = splitlk(head)
    li1 = merge_sort(head)
    li2 = merge_sort(newhead)
    # 合并链表
    return merge(li1, li2)

# 快慢指针
def splitlk(head):
    slow = head
    fast = head
    # 只判断快指针越界
    while fast.next and fast.next.next:
        slow = slow.next
        fast = fast.next.next
    # 中间指针指向慢指针的next节点
    mid = slow.next
    # 断开慢指针的next指向
    slow.next = None
    return head, mid

# 合并链表
def merge(head1, head2):
    cur = listNode()
    # 虚拟头指针指向cur
    vh = cur
    while head1 and head2:
        # cur的next指针指向较小的一方
        if head1.data < head2.data:
            cur.next = head1
            # 较小的next指针下移
            head1 = head1.next
        elif head1.data > head2.data:
            cur.next = head2
            head2 = head2.next
        # 去重逻辑，如果两个链表当前值相等，则cur指针指向其一，并且两个链表的头节点指针同时移位
        else:
            cur.next = head1
            head1 = head1.next
            head2 = head2.next
        cur = cur.next
    # cur指针直接指向剩余链表头节点
    if head1:
        cur.next = head1
    if head2:
        cur.next = head2
    return vh.next

# 二叉树初始化
class treeNode:
    def __init__(self, data=None, left=None, right=None):
        self.data = data
        self.left = None
        self.right = None


root = treeNode(6)
root.left = treeNode(3)
root.right = treeNode(8)
root.left.left = treeNode(1)
root.left.right = treeNode(4)
root.right.left = treeNode(7)
root.right.right = treeNode(9)

# 前序递归遍历，根左右
def pre_order(root):
    if not root:
        return
    else:
        print(root.data)
        pre_order(root.left)
        pre_order(root.right)

# 中序递归遍历，左根右
def in_order(root):
    if not root:
        return
    else:
        in_order(root.left)
        print(root.data)
        in_order(root.right)

# 后序递归遍历，左右根
def post_order(root):
    if not root:
        return
    else:
        post_order(root.left)
        post_order(root.right)
        print(root.data)

# 前序非递归遍历
def pre_order_norecursive(root):
    stack = []
    res = []
    # 当栈不为空或者根节点不为空时，遍历顺序为，先循环遍历左子树（打印根节点-> 左子树入栈）
    while len(stack) > 0 or root:
        while root:
            res.append(root.data)
            stack.append(root)
            root = root.left
        # 左子树入栈完成，则弹出一个，且遍历弹出节点的右子树
        root = stack.pop()
        root = root.right
    return res

# 后续遍历的非递归实现，入栈顺序与前序相反，整个结果需要reverse
def post_order_norecursive(root):
    stack = []
    res = []
    while len(stack) > 0 or root:
        # 入栈顺序为根右左，循环入栈右子树，（打印根节点->入栈右子树）
        while root:
            res.append(root.data)
            stack.append(root)
            root = root.right
        # 右子树遍历结束后，弹出一个，并且遍历弹出节点的左子树
        root = stack.pop()
        root = root.left
    # 翻转整个列表
    res.reverse()
    return res

# 中序遍历的非递归实现
def in_order_norecursive(root):
    stack = []
    res = []
    while len(stack) > 0 or root:
        # 循环遍历左子树，左子树入栈不打印
        while root:
            stack.append(root)
            root = root.left
        # 左子树遍历完毕，弹出一个节点同时打印，并且遍历该节点的右子树
        root = stack.pop()
        res.append(root.data)
        root = root.right
    return res


print(in_order_norecursive(root))


# res = post_order_norecursive(root)
# for i in range(len(res) -1, -1, -1):
#     print(res[i])


# h1 = create_linkedlist_head(abc)
# while h1:
#     print(h1.data)
#     h1 = h1.next


# h = merge_sort(h1)
# while h:
#     print(h.data)
#     h = h.next

# 最大子序和
li = [-2, 1, -3, 4, -1, 2, 1, -5, 4]

# 动态规划问题，dp[i]是第i个元素加入后的最大子序和，如果dp[i-1] >0，那么dp[i] = dp[i -1] + li[i] ，否则dp[i] = li[i]
def get_max_sum(li):
    dp = [0 for _ in range(len(li))]
    dp[0] = li[0]
    for i in range(0, len(li)):
        if dp[i - 1] > 0:
            dp[i] = dp[i - 1] + li[i]
        else:
            dp[i] = li[i]
    return max(dp)


# 输出括号，深度优先递归算法
n = 3
def generatebrackets(n):
    res = []
    dfs(n, 0, 0, res, "")
    return res

# left:左括号个数，right：右括号个数，s：当前拼接的字符串，res：结果集
def dfs(n, left, right, res, s):
    # 左括号个数超出最大值，退出
    if left >n:
        return
    # 左右括号数相等，则加入结果集
    if left == n and right == n:
        return res.append(s)
    # 左括号数小于最大值，则增加左括号
    if left < n:
        dfs(n, left + 1, right, res, s+"(")
    # 右括号数小于左括号，则增加右括号
    if left > right:
        dfs(n, left, right + 1, res, s+")")


nums = [1, 3, -1, -3, 5, 3, 6, 7]
k = 3
from collections import deque

# 滑动窗口，最大值
def maxSlidingWindow(nums, k):
    """
    :type nums: List[int]
    :type k: int
    :rtype: List[int]
    """
    res = []
    # 双向队列，单调递减，右进左出，队首q[0]在左，队尾q[-1]在右
    q = deque()
    for i, val in enumerate(nums):
        # 如果队列不为空（队列中存储的是列表中元素的下标），且队首的元素下标已经超出窗口最大范围，则弹出队首
        if q and q[0] == i - k:
            q.popleft()
        # 如果队列不为空，且队尾元素值小于当前元素，则从队尾弹出，直到当前元素小
        while q and nums[q[-1]] < val:
            q.pop()
        # 当前元素进队
        q.append(i)
        # 如果遍历的元素下标已经超过窗口最大值，表示动态窗口已经达到固定窗口宽度，开始往结果集合加入队首最大值
        if i >= k - 1:
            res.append(nums[q[0]])
    return res


print(maxSlidingWindow(nums, k))


from collections import deque


class Solution(object):
    # 单向队列，层序遍历
    def maxDepth(self, root):
        """
        :type root: TreeNode
        :rtype: int
        """
        if not root:
            return 0
        q = deque()
        # 根节点初始化入队
        q.append(root)
        depth = 0
        # 两层循环，外层队中每个节点都要循环一次
        while len(q) > 0:
            # 记录当前外层循环下 队列中的元素个数，表示每层的节点数
            num = len(q)
            # 将每层的元素循环出队，并且顺序遍历左子树和右子树，同时入队
            while num > 0 and len(q) > 0:
                root = q.popleft()
                num -= 1
                if root.left:
                    q.append(root.left)
                if root.right:
                    q.append(root.right)
            depth += 1
        return depth

# 列表的归并排序
def merge_sort2(li, left, right):
    if left < right:
        mid = (left + right) // 2
        merge_sort2(li, left, mid)
        merge_sort2(li, mid + 1, right)
        mergelist(li, left, mid, right)


def mergelist(li, left, mid, right):
    i = left
    j = mid + 1
    tmp = []
    while i <= mid and j <= right:
        if li[i] < li[j]:
            tmp.append(li[i])
            i += 1
        else:
            tmp.append(li[j])
            j += 1
    while i <= mid:
        tmp.append(li[i])
        i += 1
    while j <= right:
        tmp.append(li[j])
        j += 1
    li[left : right + 1] = tmp


a='abcdf'
print(a[0])
print(a[0].isalpha())