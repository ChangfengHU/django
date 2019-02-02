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
            print ('\t'.join([res[0], res[1], res[2]]))
            a = '\t'.join([res[0], res[1], res[2]])
            fp.write(a+'\n')
        fp.close()
        return outfile_DP



def getTokenTOIdDate(infile_DP):
    t_set = set()
    output_TTI = './04_tokenToid.txt'
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



def getUISIdDate():
    output = '05_user_item_score.txt'
    fp = open(output, 'w')
    # fp = open(output, "w")

    user_dict = {}
    with open('./04_tokenToid.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                # print '5'
                continue
            id = ss[0].strip()
            token = ss[1].strip()
            user_dict[token] = id
    item_dict = {}
    with open('./01_basedate.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')

            if len(ss) != 2:
                print (len(ss))
                continue
            id = ss[0].strip()
            item = ss[1].strip()
            item_dict[item] = id

    with open('./03_daopai.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 3:
                # print '2'
                continue
            u = ss[0].strip()
            i = ss[1].strip()
            s = ss[2].strip()
            if u not in user_dict:
                print ('1')
                continue
            if i not in item_dict:
                print('3')
                continue
            u_id = user_dict[u]
            i_id = item_dict[i]
            a = '\t'.join([u_id, i_id, s])
            fp.write(a + "\n")
    fp.close()
    return output


def getIUSIdDate(input_UIS):
    output = '06_item_user_score.txt'
    fp = open(output, 'w')
    # fp = open(output, "w")
    iuslist = []
    with open(input_UIS, 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 3:
                continue
            u = ss[0].strip()
            i = ss[1].strip()
            s = ss[2].strip()
            iuslist.append((i, u, s))
        newlist = sorted(iuslist, key=lambda x: x[0], reverse=True)
        for res in newlist:
            a = '\t'.join([res[0], res[1], res[2]])
            fp.write(a + "\n")
    fp.close()
    return output


def getUISAverageDate(input_IUS):
    output = '07_user_item_score_average.txt'
    fp = open(output, 'w')
    import math
    # fp = open(output, "w")
    cur_i = None
    u_list = []
    sum1 = 0
    u_i_s_list=[]
    with open(input_IUS, 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 3:
                continue
            i = ss[0].strip()
            u = ss[1].strip()
            s = ss[2].strip()
            if cur_i == None:
                cur_i = i

            if cur_i != i:
                for u_s1 in u_list:
                    u1 = u_s1[0].strip()
                    s1 = float(u_s1[1])
                    w1 = pow(s1, 2)
                    sum1 += float(w1)
                res = math.sqrt(sum1)
                for u_s in u_list:
                    u2 = u_s[0].strip()
                    s2 = float(u_s[1])
                    news = float(s2 / res)
                    # print '\t'.join([u,cur_i,news])
                    # a='\t'.join([u,cur_i,str(news)])
                    u_i_s_list.append((u_s[0], cur_i, str(news)))
                    # a = '\t'.join([u_s[0], cur_i, str(news)])
                    #
                    # fp.write(a + "\n")
                u_list = []
                cur_i = i
            u_list.append((u, s))

        for u_s1 in u_list:
            u3 = u_s1[0].strip()
            s3= float(u_s1[1])
            w3 = pow(s1, 2)
            sum1 += float(w3)
        res = math.sqrt(sum1)
        for u_s in u_list:
            u4 = u_s[0].strip()
            s4 = float(u_s[1])
            news4 = float(4 / res)
            # print '\t'.join([u,cur_i,news])
            # a='\t'.join([u,cur_i,str(news)])
            u_i_s_list.append((u_s[0], cur_i, str(news4)))

        newlist = sorted(u_i_s_list, key=lambda x: x[0], reverse=True)
        for res in newlist:
            a = '\t'.join([res[0], res[1], res[2]])
            fp.write(a + "\n")
    fp.close()
    return output

def getItemTOItemSimilarDate(input_UISAverage):
    cur_user = None
    item_score_list = []
    output = '08_item_item_similar.txt'
    fp = open(output, 'w')
    with open(input_UISAverage, 'r') as fd:
        for line in fd:
            user, item, score = line.strip().split("\t")
            if cur_user == None:
                cur_user = user
            if cur_user != user:
                if len(item_score_list)>35:
                    print(len(item_score_list))
                item_score_list = sorted(item_score_list, key=lambda x: x[0], reverse=True)[0:35]
                # if len(item_score_list)>2:
                # print '\t'.join(["s:",str(len(item_score_list))])
                for i in range(0, len(item_score_list) - 1):
                    for j in range(i + 1, len(item_score_list)):
                        item_a, score_a = item_score_list[i]
                        item_b, score_b = item_score_list[j]

                        a = "%s\t%s\t%s" % (item_a, item_b, score_a * score_b)
                        b = "%s\t%s\t%s" % (item_b, item_a, score_a * score_b)
                        fp.write(a + "\n")
                        fp.write(b + "\n")
                        # print "%s\t%s\t%s" % (item_a, item_b, score_a * score_b)
                        # print "%s\t%s\t%s" % (item_b, item_a, score_a * score_b)
                item_score_list = []
                cur_user = user
            item_score_list.append((item, float(score)))
        item_score_list = sorted(item_score_list, key=lambda x: x[0], reverse=True)[0:35]
        for i in range(0, len(item_score_list) - 1):
            for j in range(i + 1, len(item_score_list)):
                item_a, score_a = item_score_list[i]
                item_b, score_b = item_score_list[j]
                a = "%s\t%s\t%s" % (item_a, item_b, score_a * score_b)
                b = "%s\t%s\t%s" % (item_b, item_a, score_a * score_b)
                fp.write(a + "\n")
                fp.write(b + "\n")
        fp.close()
        return output


def getIBindIScoreDate(output_UISAverage):
    output = '09_item_bind_item_score.txt'
    fp = open(output, 'w')
    uislist = []
    count=0
    with open(output_UISAverage, 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 3:
                continue
            i1 = ss[0].strip()
            i2 = ss[1].strip()
            s = ss[2].strip()
            if i1 == i2:
                count+=1
                continue
            uislist.append((i1 + "*" + i2, s))
        newlist = sorted(uislist, key=lambda x: x[0], reverse=True)
        for res in newlist:
            a = '\t'.join([res[0], res[1]])
            fp.write(a + "\n")
    fp.close()
    print(count)
    return output

def getIISIdDate(input_IBIS):
    cur_ii_pair = None
    score = 0.0
    output = '10_item_item_score.txt'
    fp = open(output, 'w')
    ss = ''
    uislist = []
    with open(input_IBIS, 'r') as fd:
        for line in fd:
            ii_pair, s = line.strip().split("\t")
            if not cur_ii_pair:
                cur_ii_pair = ii_pair
            if ii_pair != cur_ii_pair:
                # item_a, item_b = cur_ii_pair.split('')
                ss = cur_ii_pair.split('*')

                if len(ss) != 2:
                    continue
                item_a, item_b = ss
                # print "%s\t%s\t%s" % (item_a, item_b, score)
                a = '\t'.join([item_a, item_b, str(score)])
                fp.write(a + "\n")
                cur_ii_pair = ii_pair
                score = 0.0

            score += float(s)

        ss = cur_ii_pair.split('*')
        item_a, item_b = ss
        a = '\t'.join([item_a, item_b, str(score)])
        fp.write(a + "\n")
        fp.close()
        return output


def getIISGroupDate(input_IIS):
    current_token = None
    item_w_list = []
    res_list = []
    i = 0
    output = '11_item_items_group.txt'
    fp = open(output, 'w')
    with open(input_IIS, 'r') as fd:
        for line in fd:
            i += 1
            # if i > 50000:
            #     print "daile"
            #     break
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
        return output

def testResult(recommendList):
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
    item_items_dict = {}
    with open('./11_item_items_group.txt', 'r') as fd:
        for line in fd:
            newline = line
            ss = newline.strip().split('\t')
            if len(ss) != 2:
                continue
            item = ss[0].strip()
            item_w = ss[1].strip()
            item_items_dict[item] = item_w
    # content = sys.argv[1]
    tokenid_weight_list = []
    final_list = []
    for conment in recommendList:
        # print t_id
        a=item_id_dict[0]
        print("*&&*&")
        print(a)
        # print(conment)
        if conment not in item_id_dict:
            print ("not have ")
            continue
        id = item_id_dict[conment]
        items_score=item_items_dict[id]
        item_score = items_score.strip().split('#')
        for res in item_score:
            ss = res.strip().split('*')
            if len(ss) != 2:
                continue
            item_id, w = ss
            if item_id not in item_id_dict:
                continue
            name = item_id_dict[item_id]
            rw=float(w)
            final_list.append((name,rw))
    new_final_list = sorted(final_list, key=lambda x: x[1], reverse=True)[0:10]
    title_list=[]
    for res in new_final_list:
        title_list.append(res[0])
        # print '\t'.join([res[0], str(res[1])])
    return title_list


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
    for i in range(10):
        randomNum = random.randint(1, 5391)
        title=titleList[randomNum]
        selectTitleList.append(title)
    return selectTitleList