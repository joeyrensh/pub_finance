from distutils.log import error
import random
from tkinter import Y

from numpy import matrix

li = list(range(10))
val = 7

# 线性查找
def linear_search(li, val):
    for ind, v in enumerate(li):
        if v == val:
            return ind
    return -1

# 二分查找
def binary_search(li, val):
    left = 0
    right = len(li) - 1
    while left < right:
        mid = (left + right) // 2
        if li[mid] == val:
            return mid
        elif li[mid] < val:
            left = mid + 1
        else:
            right = mid - 1
    return -1

# 冒泡排序，双指针，慢指针移动一次，快指针从i+1移动到列表尾部，相邻元素比较，较大的后移
# 依次类推，直到慢指针遍历完毕
def bubble_sort(li):
    for i in range(len(li) - 1):
        exchange_flag = False
        for j in range(len(li) - 1 - i):
            if li[j] < li[j + 1]:
                li[j], li[j + 1] = li[j + 1], li[j]
                exchange_flag = True
        if exchange_flag == False:
            return

# 选择排序，双指针，假设当前慢指针元素最小，然后依次与后面每个元素比较，找到最小值，然后与当前慢指针元素互换
def select_sort(li):
    for i in range(len(li) - 1):
        min_loc = i
        for j in range(i + 1, len(li)):
            if li[j] < li[min_loc]:
                min_loc = j
        if li[i] != li[min_loc]:
            li[i], li[min_loc] = li[min_loc], li[i]


def insert_sort(li):
    for i in range(1, len(li)):
        j = i - 1
        key = li[i]
        while j >= 0 and li[j] > key:
            li[j + 1] = li[j]
            j -= 1
        li[j + 1] = key

# 快速排序
def quick_sort(li, left, right):
    if left < right:
        mid = partition(li, left, right)
        quick_sort(li, mid + 1, right)
        quick_sort(li, left, mid - 1)


def partition(data, left, right):
    key = data[left]
    while left < right:
        while data[right] >= key and left < right:
            right -= 1
        data[left] = data[right]
        while data[left] <= key and left < right:
            left += 1
        data[right] = data[left]
    data[left] = key
    return left


def sift(li, root, last_leaf):
    i = root
    j = 2 * i + 1
    tmp = li[i]
    while j <= last_leaf:
        if j + 1 <= last_leaf and li[j + 1] > li[j]:
            j = j + 1
        if li[j] > tmp:
            li[i] = li[j]
            i = j
            j = 2 * i + 1
        else:
            li[i] = tmp
            break
    li[i] = tmp


def heap_sort(li):
    n = len(li)
    for i in range(n - 2 // 2, -1, -1):
        sift(li, i, n - 1)
    print(li)
    for j in range(n - 1, -1, -1):
        li[0], li[j] = li[j], li[0]
        sift(li, 0, j - 1)
    print(li)


def merge(li, left, mid, right):
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


def merge_sort(li, left, right):
    if left < right:
        mid = (left + right) // 2
        merge_sort(li, left, mid)
        merge_sort(li, mid + 1, right)
        merge(li, left, mid, right)

li = [1, 2, 3, 2, 5, 6]
merge_sort(li, 0, len(li)- 1)
print(li)


def insert_sort_sub(li, gap):
    for i in range(gap, len(li)):
        j = i - gap
        key = li[i]
        while j >= 0 and li[j] > key:
            li[j + gap] = li[j]
            j -= gap
        li[j + gap] = key


def shell_sort(li, val):
    gap = val
    for i in range(gap, len(li)):
        insert_sort_sub(li, gap)
        gap = gap // 2

# 计数排序
def count_sort(li, max_count=100):
    count = [0 for h in range(max_count + 1)]
    for i in li:
        count[i] += 1
    li.clear()
    for ind, val in enumerate(count):
        for j in range(val):
            li.append(ind)


li = [h for h in range(10)]
random.shuffle(li)
# print(li)


def radix_sort(li):
    max_num = max(li)
    it = 0
    while 10 ** it <= max_num:
        buckets = [[] for h in range(10)]
        for var in li:
            digit = (var // 10 ** it) % 10
            buckets[digit].append(var)
        li.clear()
        for buc in buckets:
            li.extend(buc)
        it += 1


matrix = [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10, 11, 12, 12, 13]]
target = 9


def get_by_2d_binary_search(matrix, target):
    m = len(matrix)
    n = len(matrix[0])
    if m == 0 or n == 0:
        return -1
    left = 0
    right = m * n - 1
    while left < right:
        mid = (left + right) // 2
        row = mid // n
        col = mid % n
        if matrix[row][col] == target:
            return [row, col]
        elif matrix[row][col] > target:
            right = mid - 1
        else:
            left = mid + 1


class Stack:
    def __init__(self):
        self.stack = []

    def push(self, s):
        self.stack.append(s)

    def pop(self):
        self.stack.pop()

    def get_top(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    def is_empty(self):
        return len(self.stack) == 0


def brace_match(s):
    stack = Stack()
    dic = {"}": "{", "]": "[", ")": "("}
    for ch in s:
        if len(s) == 0:
            return False
        if ch in ["{", "[", "("]:
            stack.push(ch)
        else:
            if stack.is_empty():
                return False
            elif dic[ch] == stack.get_top():
                stack.pop()
            else:
                return False
    if stack.is_empty():
        return True
    else:
        return False


class Queue:
    def __init__(self, size=100):
        self.queue = [0 for i in range(size)]
        self.size = size
        self.front = 0
        self.rear = 0

    def push(self, element):
        if not self.is_filled():
            self.rear = (self.rear + 1) % self.size
            self.queue[self.rear] = element
        else:
            raise IndexError("Queue is filled.")

    def pop(self):
        if not self.is_empty():
            self.front = (self.front + 1) % self.size
            return self.queue[self.front]
        else:
            raise IndexError("Queue is empty.")

    def is_empty(self):
        return self.rear == self.front

    def is_filled(self):
        return (self.rear + 1) % self.size == self.front


maze = [
    [0, 0, 1, 1, 1, 1, 1, 1, 1, 1],
    [1, 0, 0, 1, 1, 1, 1, 1, 1, 1],
    [1, 1, 0, 0, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 0, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 0, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 0, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 0, 0, 0, 0, 0, 0, 0],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 0],
]


def maze_path(maze, x1, y1, x2, y2):
    dirs = [
        lambda x, y: (x, y + 1),  # 右
        lambda x, y: (x + 1, y),  # 下
        lambda x, y: (x, y - 1),  # 左
        lambda x, y: (x - 1, y),  # 上
    ]
    stack = []
    stack.append((x1, y1))
    while len(stack) > 0:
        cur_node = stack[-1]
        if cur_node[0] == x2 and cur_node[1] == y2:
            for s in stack:
                print(s)
            return True
        for dir in dirs:
            next_node = dir(cur_node[0], cur_node[1])
            if next_node[0] > len(maze) - 1 or next_node[1] > len(maze[0]) - 1:
                continue
            if maze[next_node[0]][next_node[1]] == 0:
                stack.append(next_node)
                maze[next_node[0]][next_node[1]] = 2
                break
        else:
            maze[next_node[0]][next_node[1]] = 2
            stack.pop()
    else:
        return False


from collections import deque
import time

maze = [
    [0, 0, 1, 1, 1, 1, 1, 1, 1, 1],
    [1, 0, 0, 0, 0, 0, 0, 0, 0, 1],
    [1, 1, 0, 0, 1, 1, 1, 1, 0, 1],
    [1, 1, 1, 0, 1, 1, 1, 1, 0, 1],
    [1, 1, 1, 0, 1, 1, 1, 1, 0, 1],
    [1, 1, 1, 0, 1, 1, 1, 1, 0, 1],
    [1, 1, 1, 0, 0, 0, 0, 0, 0, 0],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 0],
]

def maze_path_queue(maze, x1, y1, x2, y2):
    dirs = [
        lambda x, y: (x, y + 1),  # 右
        lambda x, y: (x + 1, y),  # 下
        lambda x, y: (x, y - 1),  # 左
        lambda x, y: (x - 1, y),  # 上
    ]
    q = deque()
    path = []
    q.append((x1, y1, -1))
    while len(q) > 0:
        curNode = q.popleft()
        # print(curNode)
        # time.sleep(1)
        path.append(curNode)
        if curNode[0] == x2 and curNode[1] == y2:
            print_r(path)
            return True
        for dir in dirs:
            nextNode = dir(curNode[0], curNode[1])
            if nextNode[0] > len(maze) - 1 or nextNode[1] > len(maze[0]) - 1:
                continue
            if maze[nextNode[0]][nextNode[1]] == 0:
                q.append((nextNode[0], nextNode[1], len(path) - 1))
                maze[nextNode[0]][nextNode[1]] = 2
    else:
        return False


def print_r(path):
    curNode = path[-1]
    realpath = []

    while curNode[2] != -1:
        realpath.append(curNode[0:2])
        curNode = path[curNode[2]]
    realpath.append(curNode[0:2])
    realpath.reverse()
    for node in realpath:
        print(node)

class Node:
    def __init__(self, item):
        self.item = item
        self.next = None

def create_linkedlist_head(li):
    head = Node(li[0])
    for element in li[1:]:
        node = Node(element)
        node.next = head
        head = node
    return head

def create_linkedlist_tail(li):
    head = Node(li[0])
    tail = head
    for element in li[1:]:
        node = Node(element)
        tail.next = node
        tail = node
    return head

def print_linked_list(lk):
    while lk:
        print(lk.item, end=',')
        lk = lk.next




li = [100, 50, 10, 5]
n = 376
def get_min_change(li, n):
    m = [0 for h in range(len(li))]
    for i, money in enumerate(li):
        m[i] = n // money
        n = n % money
    return (m, n)

li = [32, 94, 128, 1286, 6, 71]
lis = list(map(str, li))
# 94716321286128
def get_max_combination(li):
    for i in range(len(li) - 1):
        exchange_flag = False
        for j in range(len(li) - 1 - i):
            if li[j] + li[j + 1] < li[j + 1] + li[j]:
                li[j], li[j + 1] = li[j + 1], li[j]
                exchange_flag = True
        if exchange_flag == False:
            return





