import pandas as pd
# 保存为CSV文件
df=pd.read_csv('dataset_names.csv')
# 将'Column1'列转换为列表.

list1 = df['Dataset Name'].tolist()

# 将'Column2'列转换为列表
list2 = df['name_2'].tolist()
set1 = set(list1)
set2 = set(list2)

# 使用差集运算符找出不同的元素
difference1 = set1 - set2  # list1中有而list2中没有的元素
difference2 = set2 - set1  # list2中有而list1中没有的元素

# 将集合转换回列表
difference1_list = list(difference1)
difference2_list = list(difference2)

print("List1中有而List2中没有的元素:", difference1_list)
print("List2中有而List1中没有的元素:", difference2_list)