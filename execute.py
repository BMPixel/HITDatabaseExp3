#%% 
import base64
from IPython.display import Image, display
import matplotlib.pyplot as plt
import re

testcase1 = "SELECT [ ENAME = 'Mary' & DNAME = 'Research' ] ( EMPLOYEE JOIN DEPARTMENT )"
testcase2 = "PROJECTION [ BDATE ] ( SELECT [ ENAME = 'John' & DNAME = ' Research' ] ( EMPLOYEE JOIN DEPARTMENT) )"
testcase3 = "SELECT [ ESSN = '01' ] ( PROJECTION [ ESSN, PNAME ] ( WORKS_ON JOIN PROJECT ) )"


#%%
class Node:
    def __init__(self, node_type, params=None, children=None):
        self.node_type = node_type
        self.params = params
        self.children = children if children else []

    def __repr__(self):
        return f"{self.node_type}({', '.join(map(str, self.children))})"

def parse_query(query):
    # 匹配 SELECT、PROJECTION、JOIN 等关键词
    match = re.match(r"(SELECT|PROJECTION|JOIN)\s*(\[.*?\])?\s*\((.*?)\)", query)
    if match:
        node_type = match.group(1)
        params = match.group(2)
        if node_type in ["SELECT", "PROJECTION"]:
            child = parse_query(match.group(3))
            return Node(node_type, params, [child])
        elif node_type == "JOIN":
            left, right = re.split(r"\s*JOIN\s*", match.group(3))
            return Node(node_type, None, [parse_query(left), parse_query(right)])
    else:
        return Node(query.strip())

def can_swap(node):
    return (
        node.node_type == "SELECT"
        and node.children[0].node_type == "PROJECTION"
        or (
            node.node_type == "PROJECTION"
            and node.children[0].node_type == "SELECT"
        )
    )

def swap_nodes(node):
    if can_swap(node):
        node.node_type, node.children[0].node_type = (
            node.children[0].node_type,
            node.node_type,
        )
        node.params, node.children[0].params = (
            node.children[0].params,
            node.params,
        )
        return True
    return False

def optimize_tree(node):
    while swap_nodes(node):
        pass
    for child in node.children:
        optimize_tree(child)

# 测试

