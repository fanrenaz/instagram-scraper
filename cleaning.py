# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 23:22:30 2020

@author: Yifan Ren
"""

import json
import pandas as pd
import math
from sklearn import preprocessing
import os
from multiprocessing.dummy import Pool as ThreadPool
import warnings
warnings.filterwarnings('ignore')

def extract_node_text(i):
    try:
        i = i.get("edges")[0].get("node").get("text")
    except:
        pass
    return i

def tag_join(l):
    try:
        l = ",".join(l)
    except: pass
    return l

def json_to_df(file):
    with open(file, 'r') as f:
        js = json.load(f)
    df = pd.DataFrame(js["GraphImages"])
    df["dimensions"] = df["dimensions"].map(lambda line: str(line.get("height"))+"*"+str(line.get("width")))
    df["edge_liked_by"] = df["edge_liked_by"].map(lambda line: line.get("count"))
    df["edge_media_preview_like"] = df["edge_media_preview_like"].map(lambda line: line.get("count"))
    df["edge_media_to_caption"] = df["edge_media_to_caption"].map(extract_node_text)
    df["edge_media_to_comment"] = df["edge_media_to_comment"].map(lambda line: line.get("count"))
    df["owner"] = df["owner"].map(lambda line: line.get("id"))
    df["tags"] = df["tags"].map(tag_join)
    df["comments_disabled"] = df["comments_disabled"].map(lambda x: None if math.isnan(x) is True else x)
    df["video_view_count"] = df["video_view_count"].map(lambda x: x if type(x)==float else None)
    return df
