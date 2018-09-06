# -*- coding:utf-8 -*-
import sys
sys.path.append('../../grpc_stub/')
import xiehe_etl_pb2
import xiehe_etl_pb2_grpc
from concurrent import futures
import os
import json
import yaml
import jieba
import re
import grpc
import time

_ONE_DAY_IN_SECONDS=60*60*24

class Extract(xiehe_etl_pb2_grpc.EtlServiceServicer):
    def meticulousExtract(self, request, context):
        surgeryTime=request.surgeryTime
        sszd=request.shuQianZhenDuan
        shzd=request.shuHouZhenDuan
        ssmc=request.surgeryName
        ssys=request.surgeryDoctors
        mzfs=request.maZuiFangShi
        mzys=request.maZuiYiShi
        ssbz=request.shouShuBuZhou
        jlz=request.jiLuZhe

        jieba.load_userdict('./ys.txt')
        f_shuzhe_all = open('./yaml_file/shuzhe_all.yaml', encoding='utf-8')
        f_shuzhe_part = open('./yaml_file/shuzhe.yaml', encoding='utf-8')
        f_ssfs = open('./yaml_file/ssfs.yaml', encoding='utf-8')
        f_ssyx = open('./yaml_file/ssyx.yaml', encoding='utf-8')

        str_dh=','

        ssys_seg=jieba.cut(ssys)
        set_ssys_seg=set(ssys_seg)

        # get all doctor
        doctor_all=yaml.load(f_shuzhe_all)
        list_doctor_all=doctor_all['shuzhe_all_name']
        set_doctor_all=set(list_doctor_all)

        # get part doctor
        doctor_part=yaml.load(f_shuzhe_part)
        list_doctor_part=doctor_part['shuzhe_name']
        set_doctor_part=set(list_doctor_all)

        # get name
        shuzhe_all = set_ssys_seg&set_doctor_all
        shuzhe_part = set_ssys_seg&set_doctor_part
        surgeryDoctorOther = str_dh.join(shuzhe_all-shuzhe_part)   # get shuzhe name
        surgeryDoctorMain = str_dh.join(shuzhe_part)            # get shuzhe_qt name

        # get shoushufangshi
        ssmc_seg = jieba.cut(ssmc)
        set_ssmc_seg = set(ssmc_seg)
        ssfs_all = yaml.load(f_ssfs)
        set_ssfs_all = set(ssfs_all['ssfs'])
        surgicalMaintype = str_dh.join(set_ssmc_seg&set_ssfs_all)

        # get shoushuyaxing
        ssyx_all = yaml.load(f_ssyx)
        set_ssyx_all = set(ssyx_all['ssyx'])
        surgicalSubtype = str_dh.join(set_ssmc_seg&set_ssyx_all)

        # get cancer_size
        pattern1 = re.compile(r'\d.?\d?[×xX]\d?.?\d[×xX].{3}')
        pattern2 = re.compile(r'\d{1,}.?\d[×xX]\d{0,}.?\d?|\d{1,2}mum|\d{1,2}\.?\d{0,}[cm]')
        pattern3 = re.compile(r'\d{1,2}m{1,2}[×xX]?\d{1,2}m{1,2}[×xX]\d{1,2}m{1,2}')

        result1 = pattern1.findall(ssbz)
        result2 = pattern2.findall(ssbz)
        result3 = pattern3.findall(ssbz)
        if result1:
            tumourSize = result1[0]
        elif result3:
            tumourSize = result3[0]
        elif result2:
            tumourSize = result2[-1]
        else:
            tumourSize = ''

        return xiehe_etl_pb2.SurgeryDetail(surgeryTime=surgeryTime,surgeryDoctorMain=surgeryDoctorMain,surgeryDoctorOther=surgeryDoctorOther,surgicalMaintype=surgicalMaintype,surgicalSubtype=surgicalSubtype,tumourSize=tumourSize)



def server():
    server=grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    xiehe_etl_pb2_grpc.add_EtlServiceServicer_to_server(Extract(),server)
    server.add_insecure_port('10.100.1.145:60081')
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop()


if __name__=='__main__':
    server()