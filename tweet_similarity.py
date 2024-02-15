from konlpy.tag import Okt
from numpy import dot
from numpy.linalg import norm
import numpy as np
import re
import csv
import time
import pandas as pd
from collections import Counter

def cos_sim(a, b):
    norm_a = norm(a)
    norm_b = norm(b)

    if norm_a == 0 or norm_b == 0:
        return 0
    
    return dot(a, b)/(norm(a)*norm(b))


def make_frequency(words):
    return Counter(words)


def jac_sim(set1, set2):
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))

    if union == 0:
        return "요소 없음"
    
    similarity = intersection / union
    return similarity


# csv 파일 로딩
csv_file_path = 'C:\\Users\\qkrwo\\Desktop\\drug\\search_#dmtvape.csv'
df = pd.read_csv(csv_file_path, encoding='utf-8')

# 열 가져오기
user = df['계정_ID']
#print(user)
data_list = []
  
for index, row in df.iterrows():
    if not pd.notna(row["계정_소개글_해시태그"]):
        temp_1 = "pass"
        data_list.append(temp_1)
        continue

    data = row["계정_소개글_해시태그"].strip('[ ]').split(', ')

    data_list.append(data)
    
print(data_list)
#okt = Okt()

result = []

for i in range(len(data_list)):
    '''
    if not data_list[i] or not data_list[i][0] or user[i] in result:
        continue
    result.append(user[i])
    '''

    count = 1
    St = str(data_list[i][0])

    for y in range(i + 1, len(data_list)):
        count_1 = 1
        temp = str(data_list[y][0])

        print(f"기준 - {user[i]}계정 트윗")
        print(f"비교 - {user[y]}계정 트윗")

        num1 = i
        num2 = y
        num0 = f'{user[num1]} 계정 트윗 <-> {user[num2]} 계정 트윗'

        #temp = re.sub(r'#', '', temp)   #해시태그 분석 시 주석처리
        temp = re.sub(r'\n', '', temp)

        #v1 = okt.nouns(St)
        #v2 = okt.nouns(temp)
        v1 = St.split()
        v2 = temp.split()

        '''
        # 단어 중복제거
        feats = set(v1 + v2)

        v1_arr = np.array(make_frequency(feats, v1))
        v2_arr = np.array(make_frequency(feats, v2))
        '''
        # 단어 중복제거
        feats = set(v1 + v2)

        # 각 단어의 빈도수 구하기
        v1_freq = make_frequency(v1)
        v2_freq = make_frequency(v2)

        # 각 단어의 빈도수 벡터로 변환
        '''
        v1_arr = np.array([v1_freq[feat] for feat in feats])
        v2_arr = np.array([v2_freq[feat] for feat in feats])
        '''
        v1_arr = np.array([v1_freq.get(feat, 0) for feat in feats])
        v2_arr = np.array([v2_freq.get(feat, 0) for feat in feats])

        cs1 = cos_sim(v1_arr, v2_arr)

        print('cos : v1 <-> v2 =', cs1)

        # 문자열을 단어로 분리하여 집합으로 변환
        #set1 = set(St.split())
        #set2 = set(temp.split())
        set1 = set(v1)
        set2 = set(v2)

        # 자카드 유사도 계산
        similarity = jac_sim(set1, set2)
        result.append([num0, cs1, similarity])
        print(f"jac : v1 <-> v2 = {similarity}")
        count_1 += 1

    count += 1

# 데이터프레임 생성
df = pd.DataFrame(result, columns=['비교대상' ,'코사인 유사도', '자카드 유사도'])

# 인덱스 컬럼 추가
df.index.name = '번호'
df.reset_index(inplace=True)

# csv 파일로 저장
filename = '계정소개글해시태그#dmtvape.csv'
df.to_csv(filename, index=False, encoding='ANSI')