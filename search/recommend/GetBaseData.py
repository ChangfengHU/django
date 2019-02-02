#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/1 23:43
# @Author  : changfeng
import  sys
import  jieba
import  jieba.analyse
import  random
from    elasticsearch  import  Elasticsearch
client=Elasticsearch(hosts=["127.0.0.1"])
def  getBSDate():
    # 连接ES
    # 筛选字段
    para = {
        "_source":  "_id,title"
    }
    # size一定要大于字段所对应值得总数，不然查询出的值不全，其他参数设置我就不多说了
    array_search = client.search(index="jobbole",  doc_type="article",  params=para,  size=6000)
    jsons = array_search["hits"]["hits"]
    outfile_BS  =  './01_basedate.txt'
    fp = open(outfile_BS, 'w')
    lng = []
    # 检索字段并保存字段值
    for hits in jsons:
        try:
            write = '\t'.join([hits["_id"], hits["_source"]["title"]])
            fp.write(write + "\n")
        except:
            pass
    fp.close()
    return outfile_BS

def  getZPDate(infile_BS):
    outfile_ZP  =  './02_zhengpai.txt'
    fp  =  open(outfile_ZP,  "w")
    with  open(infile_BS,'r')  as  fd:
        i  =0
        for  line  in  fd:
            i+=1
            newline=  line
            ss=newline.strip().split('\t')
            if  len(ss)  !=  2:
                continue
            music_id=ss[0 ].strip()
            music_name=ss[1].strip()
            tokenid_weight_list = []
            for x, w in jieba.analyse.extract_tags(music_name, withWeight=True):
                tokenid_weight_list.append((x, str(w)))
            write= '\t'.join([music_id,music_name,'*'.join(['#'.join([res[0],res[1]]) for res in tokenid_weight_list])])
            fp.write(write + "\n")
    fp.close()
    return outfile_ZP


def getDPDate(infile_ZP):
    outfile_DP = '03_daopai.txt'
    fp = open(outfile_DP, 'w')
    t_n_w = []
    with open(infile_ZP, 'r') as fd:
        i = 0
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 3:
                continue
            music_id = ss[0].strip()
            music_name = ss[1].strip()
            t_w = ss[2].strip()
            twlist = t_w.strip().split('*')
            # print '\t'.join(twlist)
            for tw in twlist:
                qq = tw.strip().split('#')
                if len(qq) != 2:
                    continue
                token = qq[0].strip()
                weight = qq[1].strip()
                t_n_w.append((token, music_name, weight))
    final_rec_list = sorted(t_n_w, key=lambda x: x[0], reverse=True)

    for res in final_rec_list:
        # print '\t'.join([res[0], res[1], res[2]])
        a = '\t'.join([res[0], res[1], res[2]])
        fp.write(a+'\n')
    fp.close()
    return outfile_DP


def getDPGroupDate(infile_DP):
    current_token = None
    item_w_list = []
    res_list = []
    i = 0
    outfile_DPGP = '04_daopaigroup.txt'
    fp = open(outfile_DPGP, 'w')
    with open(infile_DP, 'r') as fd:
        for line in fd:
            i += 1
            if i > 50000:
                print ("daile")
                break
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 3:
                continue
            token = ss[0].strip()
            item = ss[1].strip()
            weight = ss[2].strip()
            # print token
            if current_token == None:
                current_token = token
            if current_token != token:
                final_rec_list = sorted(item_w_list, key=lambda x: x[1], reverse=True)
                write = '\t'.join([current_token, '#'.join(['*'.join([res[0], str(res[1])]) for res in final_rec_list])])
                fp.write(write + "\n")
                current_token = token
                item_w_list = []
            item_w_list.append((item, float(weight)))
        final_rec_list = sorted(item_w_list, key=lambda x: x[1], reverse=True)
        write = '\t'.join([token, '#'.join(['*'.join([res[0], str(res[1])]) for res in final_rec_list])])
        fp.write(write + "\n")
        fp.close()
        return outfile_DPGP


def getTokenTOIdDate(infile_DP):
    t_set = set()
    output_TTI = './05_tokenToid.txt'
    fp = open(output_TTI, "w")
    with open(infile_DP, 'r') as fd:
        i = 0
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 3:
                continue
            t = ss[0].strip()
            t_set.add(t)
    for i, t in enumerate(list(t_set)):
        write = '\t'.join([str(i), t])
        fp.write(write + "\n")
    fp.close()
    return output_TTI




def getResultAllIdDate():
    output_result_allId = './06_result_trans_all_id.txt'
    fp = open(output_result_allId, "w")
    n_w_idlist = []
    token_id_dict = {}
    with open('./05_tokenToid.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            token = ss[1].strip()
            token_id_dict[token] = id
    item_id_dict = {}
    with open('./01_basedate.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            item = ss[1].strip()
            item_id_dict[item] = id

    with open('./04_daopaigroup.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            t = ss[0].strip()
            n_w = ss[1].strip()

            if t not in token_id_dict:
                print ('1')
                continue
            t_id = token_id_dict[t]
            # print n_w
            nw_list = n_w.strip().split('#')
            n_w_idlist = []
            for nw in nw_list:
                ss = nw.strip().split('*')
                if len(ss) != 2:
                    continue
                n, w = ss
                # print n
                if n not in item_id_dict:
                    print ('2')
                    continue
                n_id = item_id_dict[n]
                n_w_idlist.append((n_id, w))
            # for res in n_w_idlist:
            # print res[0]
            print ('\t'.join([t_id, '*'.join([':'.join([res[0], res[1]]) for res in n_w_idlist])]))
            write = '\t'.join([t_id, '*'.join([':'.join([res[0], res[1]]) for res in n_w_idlist])])
            fp.write(write + "\n")
            print(write)
    fp.close()
    return output_result_allId



def testResult(recommendList):
    token_id_dict = {}
    with open('./search/recommend/05_tokenToid.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            token = ss[1].strip()
            token_id_dict[token] = id
    item_id_dict = {}
    with open('./search/recommend/01_basedate.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            item = ss[1].strip()
            item_id_dict[id] = item

    id_dict = {}
    with open('./search/recommend/06_result_trans_all_id.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            item_w = ss[1].strip()
            id_dict[id] = item_w

    # content = sys.argv[1]
    tokenid_weight_list = []
    final_dict={}
    final_list = []
    for conment in recommendList:
        for x, w in jieba.analyse.extract_tags(conment, withWeight=True):
            tokenid_weight_list.append((x, str(w)))
        for res in tokenid_weight_list:
            t = res[0]
            bw = res[1]
            if t not in token_id_dict:
                continue
            t_id = token_id_dict[t]
            # print t_id
            if t_id not in id_dict:
                continue
            n_w = id_dict[t_id]
            nwlist = n_w.strip().split('*')
            for res in nwlist:
                ss = res.strip().split(':')
                if len(ss) != 2:
                    continue
                n_id, w = ss
                if n_id not in item_id_dict:
                    continue
                name = item_id_dict[n_id]
                rw=float(bw)*float(w)
                if name in recommendList:
                    continue
                if name  not in final_dict:
                    final_dict[name]=rw
                else:
                    final_dict[name]=float(final_dict[name])+rw
    for key, value in final_dict.items():
        final_list.append((key, float(value)))

    new_final_list = sorted(final_list, key=lambda x: x[1], reverse=True)[0:5]
    final_list=[]
    for res in new_final_list:
        final_list.append(res[0])
        print ('\t'.join([res[0], str(res[1])]))
    return final_list

def getRamdomTitle():
    outfile_ZP = './search/recommend/01_basedate.txt'
    titleList=[]
    with open(outfile_ZP, 'r') as fd:
        i = 0
        for line in fd:
            i += 1
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            music_name = ss[1].strip()
            titleList.append(music_name)
    selectTitleList=[]
    for i in range(3):
        randomNum = random.randint(1, 5391)
        title=titleList[randomNum]
        selectTitleList.append(title)
    return selectTitleList

def getOwnRamdomTitle():
    outfile_ZP = './01_basedate.txt'
    titleList=[]
    with open(outfile_ZP, 'r') as fd:
        i = 0
        for line in fd:
            i += 1
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            music_name = ss[1].strip()
            titleList.append(music_name)
    selectTitleList=[]
    for i in range(1):
        randomNum = random.randint(1, 5391)
        title=titleList[randomNum]
        selectTitleList.append(title)
    return selectTitleList



def testXietongResult(recommendList):
    token_id_dict = {}
    with open('./search/recommend/05_tokenToid.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            token = ss[1].strip()
            token_id_dict[token] = id
    id_token_dict = {}
    with open('./search/recommend/01_basedate.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            item = ss[1].strip()
            id_token_dict[id] = item

    id_dict = {}
    # with open('./search/recommend/06_result_trans_all_id.txt', 'r') as fd:
    with open('./search/recommend/06_result_trans_all_id.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            item_w = ss[1].strip()
            id_dict[id] = item_w
    item_item_dict = {}
    # with open('./search/recommend/06_result_trans_all_id.txt', 'r') as fd:
    with open('./search/recommend/11_item_items_group.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            item = ss[0].strip()
            item_w = ss[1].strip()
            item_item_dict[item] = item_w

    # content = sys.argv[1]
    tokenid_weight_list = []
    final_dict={}
    final_list = []
    for conment in recommendList:
        for x, w in jieba.analyse.extract_tags(conment, withWeight=True):
            tokenid_weight_list.append((x, str(w)))
        for res in tokenid_weight_list:
            t = res[0]
            bw = res[1]
            if t not in token_id_dict:
                continue
            t_id = token_id_dict[t]
            # print t_id
            if t_id not in id_dict:
                continue
            n_w = id_dict[t_id]
            nwlist = n_w.strip().split('*')
            for res in nwlist:
                ss = res.strip().split(':')
                if len(ss) != 2:
                    continue
                n_id, w = ss
                if n_id not in id_token_dict:
                    continue
                if n_id in item_item_dict:
                    item_scores=item_item_dict[n_id]
                    item_socre = item_scores.strip().split('#')[0:10]
                    for res in item_socre:
                        item_socre = res.strip().split('*')
                        if len(item_socre) != 2:
                            continue
                        itemId, score = item_socre
                        nw = float(w) * float(score)*float(bw)
                        if itemId not in id_token_dict:
                            continue
                        name = id_token_dict[itemId]
                        if name in recommendList:
                            continue
                        if name not in final_dict:
                            final_dict[name]=nw
                        else:
                            final_dict[name]=float(final_dict[name])+nw
    for key, value in final_dict.items():
        final_list.append((key, float(value)))
    new_final_list = sorted(final_list, key=lambda x: x[1], reverse=True)[0:5]
    title_list=[]
    for res in new_final_list:
        title_list.append(res[0])
        print ('\t'.join([res[0], str(res[1])]))
    return title_list

def testOwnXietongResult(recommendList):
    token_id_dict = {}
    with open('./05_tokenToid.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            token = ss[1].strip()
            token_id_dict[token] = id
    id_token_dict = {}
    with open('./01_basedate.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            item = ss[1].strip()
            id_token_dict[id] = item

    id_dict = {}
    # with open('./search/recommend/06_result_trans_all_id.txt', 'r') as fd:
    with open('./06_result_trans_all_id.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            item_w = ss[1].strip()
            id_dict[id] = item_w
    item_item_dict = {}
    # with open('./search/recommend/06_result_trans_all_id.txt', 'r') as fd:
    with open('./11_item_items_group.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            item = ss[0].strip()
            item_w = ss[1].strip()
            item_item_dict[item] = item_w

    # content = sys.argv[1]
    tokenid_weight_list = []
    final_dict={}
    final_list = []
    for conment in recommendList:
        for x, w in jieba.analyse.extract_tags(conment, withWeight=True):
            tokenid_weight_list.append((x, str(w)))
        for res in tokenid_weight_list:
            t = res[0]
            bw = res[1]
            if t not in token_id_dict:
                continue
            t_id = token_id_dict[t]
            # print t_id
            if t_id not in id_dict:
                continue
            n_w = id_dict[t_id]
            nwlist = n_w.strip().split('*')
            for res in nwlist:
                ss = res.strip().split(':')
                if len(ss) != 2:
                    continue
                n_id, w = ss
                if n_id not in id_token_dict:
                    continue
                if n_id in item_item_dict:
                    item_scores=item_item_dict[n_id]
                    item_socre = item_scores.strip().split('#')[0:10]
                    for res in item_socre:
                        item_socre = res.strip().split('*')
                        if len(item_socre) != 2:
                            continue
                        itemId, score = item_socre
                        nw = float(w) * float(score)*float(bw)
                        if itemId not in id_token_dict:
                            continue
                        name = id_token_dict[itemId]
                        if name in recommendList:
                            continue
                        if name not in final_dict:
                            final_dict[name]=nw
                        else:
                            final_dict[name]=float(final_dict[name])+nw
    for key, value in final_dict.items():
        final_list.append((key, float(value)))
    new_final_list = sorted(final_list, key=lambda x: x[1], reverse=True)[0:5]
    title_list=[]
    for res in new_final_list:
        title_list.append(res[0])
        print ('\t'.join([res[0], str(res[1])]))
    return title_list

def testOwnResult(recommendList):
    token_id_dict = {}
    with open('./05_tokenToid.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            token = ss[1].strip()
            token_id_dict[token] = id
    item_id_dict = {}
    with open('./01_basedate.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            item = ss[1].strip()
            item_id_dict[id] = item

    id_dict = {}
    with open('./06_result_trans_all_id.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            id = ss[0].strip()
            item_w = ss[1].strip()
            id_dict[id] = item_w

    # content = sys.argv[1]
    tokenid_weight_list = []
    final_dict={}
    final_list = []
    for conment in recommendList:
        for x, w in jieba.analyse.extract_tags(conment, withWeight=True):
            tokenid_weight_list.append((x, str(w)))
        for res in tokenid_weight_list:
            t = res[0]
            bw = res[1]
            if t not in token_id_dict:
                continue
            t_id = token_id_dict[t]
            # print t_id
            if t_id not in id_dict:
                continue
            n_w = id_dict[t_id]
            nwlist = n_w.strip().split('*')
            for res in nwlist:
                ss = res.strip().split(':')
                if len(ss) != 2:
                    continue
                n_id, w = ss
                if n_id not in item_id_dict:
                    continue
                name = item_id_dict[n_id]
                rw=float(bw)*float(w)/10
                if name in recommendList:
                    continue
                if name  not in final_dict:
                    final_dict[name]=rw
                else:
                    final_dict[name]=float(final_dict[name])+rw
    for key, value in final_dict.items():
        final_list.append((key, float(value)))

    new_final_list = sorted(final_list, key=lambda x: x[1], reverse=True)[0:5]
    final_list=[]
    for res in new_final_list:
        final_list.append(res[0])
        print ('\t'.join([res[0], str(res[1])]))
    return final_list