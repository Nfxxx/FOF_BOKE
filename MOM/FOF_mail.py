import imaplib
import email
import email.header
import pandas as pd
import os
import re
import warnings

warnings.filterwarnings("ignore")

path = 'email_FOF'
if os.path.exists(path):
    pass
else:
    os.mkdir(path)


def decode_mime_words(s):
    return u''.join(
        word.decode(encoding or 'utf8') if isinstance(word, bytes) else word
        for word, encoding in email.header.decode_header(s))


def read_csv(daily):
    out_df = pd.DataFrame(columns=['文件名称'])
    imaplib.Commands["ID"] = "NONAUTH"
    # mail = imaplib.IMAP4_SSL(host="imaphz.qiye.163.com", port=993)
    mail = imaplib.IMAP4_SSL(host="imap.feishu.cn")
    mail.login("fof_gz@bokesimu.com", "qrZGVSvqhIQ4jKpP")
    mail._simple_command("ID", '("name" "test" "version" "0.0.1")')
    # mail.select()
    mail.select('INBOX')
    row_code = f'SINCE {daily}'
    result, data = mail.search(None, row_code)
    ids = data[0].split()

    for id in ids:
        try:
            result, data = mail.fetch(id, '(RFC822)')
            raw_email = data[0][1]
            # print(raw_email)
            email_message = email.message_from_bytes(raw_email)
            subject = email_message['Subject']
            sender = email.utils.parseaddr(email_message['From'])[1]

            try:
                body = ""
                for part in email_message.walk():
                    # 判断内容类型
                    if part.get_content_type() == 'text/html':  # 文本/HTML
                        # 如果你想要HTML正文，则处理这部分
                        body += part.get_payload(decode=True).decode()
                    # 还可以处理其他MIME类型，如附件等
                T = pd.read_html(body)
                table=T[0]
                table.columns = table.values.tolist()[0]
                table.drop([0], inplace=True)
                table=table.drop(columns=['资产净值'])
                name =  'z_手录' + decode_mime_words(subject)
                table.to_excel(f'{path}/{name}.xlsx')
            except:
                pass
            if 'citicsf' in sender:
                if '深耕5号' in decode_mime_words(subject):
                    body = ""
                    for part in email_message.walk():
                        # 判断内容类型
                        if part.get_content_type() == 'text/html':  # 文本/HTML
                            # 如果你想要HTML正文，则处理这部分
                            body += part.get_payload(decode=True).decode()
                        # 还可以处理其他MIME类型，如附件等
                    T = pd.read_html(body)
                    table = T[0]
                    table.columns = table.values.tolist()[0]
                    table.drop([0], inplace=True)
                    table = table.drop(columns=['资产净值'])
                    table['基金代码'] = 'STD841'
                    table['基金名称'] = '中信期货价值深耕5号'
                    name = 'z_手录' + decode_mime_words(subject)
                    table.to_excel(f'{path}/{name}.xlsx')
            if 'htsc' in sender:
                try:
                    if '磐松中证500' in decode_mime_words(subject):
                        body = ""
                        for part in email_message.walk():
                            # 判断内容类型
                            if part.get_content_type() == 'text/html':  # 文本/HTML
                                # 如果你想要HTML正文，则处理这部分
                                body += part.get_payload(decode=True).decode()
                            # 还可以处理其他MIME类型，如附件等
                        T = pd.read_html(body)
                        table = T[0]
                        print(table)

                        name = 'z_手录_' + decode_mime_words(subject)
                        table.to_excel(f'{path}/{name}.xlsx')
                except:
                    pass
            try:
                if email_message.get_content_maintype() == 'multipart':
                    for part in email_message.walk():
                        if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
                            continue
                        filename = part.get_filename()
                        name = decode_mime_words(filename)
                        if ('.xls' or 'xlsx') in name:

                            if 'cmsc' in sender:  # 招商
                                pas='a'
                                # if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
                            elif 'citi' in sender:  # 中信
                                pas = 'b'
                                # if '虚拟' not in name or '基金净值表现估算' in name:
                                #     if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
                            elif 'gtja' in sender:  # 国君
                                pas='c'
                                # if '流水' not in name:
                                #     if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                    # print(name)
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
                            elif 'guos' in sender:  # 国信
                                pas='d'
                                # if '流水' not in name:
                                #     if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                        # print(name)
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
                            elif 'ebsc' in sender:  # 光大
                                pas='e'
                                # if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
                            elif 'htsc' in sender:  # 华泰
                                pas='f'
                                # if '流水' not in name:
                                #     if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                        f.write(part.get_payload(decode=True))
                            elif 'cnht' in sender:  # 恒泰
                                pas='g'
                                # if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
                            elif 'cicc' in sender:  # 中金
                                pas='h'
                                # if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
                            elif 'gjdf' in sender:  # 国金道富
                                pas='I'
                                # if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
                            elif 'csc' in sender:  # 中信建投
                                pas='J'
                                # if '流水' not in name:
                                #     if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                        # print(name)
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                        f.write(part.get_payload(decode=True))
                            elif 'gf' in sender:  # 广发
                                pas = 'K'
                                # if '流水' not in name:
                                #     if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                        f.write(part.get_payload(decode=True))
                            elif 'xyzq' in sender:  # 兴业
                                pas='l'
                                # if '流水' not in name:
                                #     if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                        # print(name)
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                        f.write(part.get_payload(decode=True))
                            else:
                                pas='z'
                                # if '集合计划每日净值表' in name:
                                name = pas + '_' + decode_mime_words(subject) + name
                                    # print(name)
                                with open(f'{path}/{pas}_{name}', 'wb') as f:
                                    f.write(part.get_payload(decode=True))
            except Exception as e:
                print(e)
        except:
            pass
    out_df.to_excel('单日全部文件名称.xlsx')


def fuzzy_search(pattern, lst):
    result = []

    for item in lst:
        if re.search(pattern, str(item)):
            result.append(item)

    return result


def remove_brackets(string):
    pattern = r'\([^()]*\)'  # 匹配括号及其内容
    result = re.sub(pattern, '', string)  # 将匹配到的部分替换为空字符串
    return result

#按照一定顺序 查找累计单位净值字段名称
def find_net_name(net_list):
    net_list = [x for x in net_list if x[0] != '母']
    net_value_name = ['单位净值', '资产份额净值','虚拟净值', '实际净值','虚拟后净值','资产份额净值(元)', '计提前单位净值','虚拟净值提取前单位净值']
    add_value_name = ['累计单位净值', '资产份额累计净值', '实际累计净值', '累计净值', '计提前累计净值','虚拟净值提取前累计单位净值','虚拟净值提取前单位净值','资产份额累计净值(元)']
    for i in net_value_name:
        try:
            a = fuzzy_search(i, net_list)[0]
            break
        except:
            pass
    for j in add_value_name:

        try:
            b = fuzzy_search(j, net_list)[0]
            break
        except:
            pass
    return [a, b]


def find_value():
    out_df = pd.DataFrame(columns=['UID', '日期', '产品代码', '产品名称', '单位净值', '累计净值', '来源文件'])
    not_read = pd.DataFrame(columns=['来源文件'])
    folder_path = path

    for file_name in os.listdir(folder_path):
        if '三级科目' in file_name:
            pass
        elif '3级' in file_name:
            pass
        elif '4级' in file_name:
            pass
        elif '-3-' in file_name:
            pass
        else:
            file_path = os.path.join(folder_path, file_name)
            if '计划每日' in file_name:
                try:
                    D = pd.read_excel(file_path, header=2)
                    for ii in range(D.shape[0]):
                        rq1 = D['日期'].iloc[ii]
                        rq = rq1.replace('年', '-').replace('月', '-').replace('日', '')
                        UnderlyingName = D['产品名称'].iloc[ii]
                        FundCode = D['产品代码'].iloc[ii]
                        NetValue = round(float(D['单位净值'].iloc[ii]), 4)
                        AccNetValue = round(float(D['累计单位净值'].iloc[ii]), 4)
                        UID = rq + '_' + FundCode
                        a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                        out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                except:
                    pass

            elif '发送每日净值信息' not in file_name and '虚拟' not in file_name and '估值表' not in file_name:
                try:
                    df = pd.read_excel(file_path).head(n=1)
                    # print(df)
                    index_list = df.columns.values
                    # print(index_list)
                    daily = str(df[fuzzy_search('日期', index_list)[0]].values[0])
                    if daily[4] == '年':
                        daily = daily.replace('年', '-').replace('月', '-').replace('日', '')
                    if daily[4] == '/':
                        daily = daily.replace('/', '-')
                    elif daily[4] != '-':
                        daily = f"{daily[:4]}-{daily[4:6]}-{daily[6:8]}"
                    try:
                        ID = remove_brackets(df[fuzzy_search('产品代码', index_list)[0]].values[0])
                    except:
                        ID = remove_brackets(df[fuzzy_search('代码', index_list)[0]].values[0])
                    UID = daily + '_' + ID
                    try:
                        name = df[fuzzy_search('基金名称', index_list)[0]].values[0]
                    except:
                        try:
                            name = df[fuzzy_search('产品名称', index_list)[0]].values[0]
                        except:
                            name = df[fuzzy_search('名称', index_list)[0]].values[0]
                    net_list = fuzzy_search('净值', index_list)
                    net_name = find_net_name(net_list)
                    # print(net_name[0])
                    netValue = df[net_name[0]].values[0]
                    addNetValue = df[net_name[1]].values[0]
                    # print(UID,daily,ID,name,netValue,addNetValue,file_path)
                    a = [UID, daily, ID, name, netValue, addNetValue, file_path]
                    out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                except :
                    try:
                        if 'e_'in file_path:
                            if '【' in file_path:
                                tmp = D.iloc[1, :]
                                tmp.index = D.iloc[0, :].tolist()
                                rq0 = tmp['净值日期']
                                rq = rq0[:4] + '-' + rq0[4:6] + '-' + rq0[-2:]
                                UnderlyingName = tmp['基金名称']
                                NetValue = round(float(tmp['试算前单位净值']), 4)
                                AccNetValue = round(float(tmp['试算前累计单位净值']), 4)
                                FundCode = tmp['基金代码']
                                UID = rq + '_' + FundCode
                                a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                                out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                            else:
                                tmp = D.iloc[1, :]
                                rq = tmp[tmp.str.contains('-').fillna(False)].values[0][-10:]
                                UnderlyingName = D.iloc[5, 0]
                                NetValue = round(float(D.iloc[5, -2]), 4)
                                AccNetValue = round(float(D.iloc[5, -1]), 4)
                                FundCode = D.iloc[5, -3]
                                UID = rq + '_' + FundCode
                                a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                                out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                    except Exception as e:
                        print(e)
                        print(file_name)

            elif '虚拟' in file_name:
                try:
                    #
                    if '虚拟业绩报酬' in file_name:
                        df = pd.read_excel(file_path, header=1).head(n=1)
                    else:
                        if 'K_' in file_name:
                            # print(file_name)
                            try:
                                df = pd.read_excel(file_path,header=2).head(1)
                                if 'K_K_世纪前沿指数增强专' in file_name:
                                    df['产品代码']='SSS447'

                            except:
                                df = pd.read_excel(file_path).head(n=1)
                        else:
                            df = pd.read_excel(file_path).head(n=1)
                    index_list = df.columns.values
                    try:
                        daily = str(df[fuzzy_search('日期', index_list)[0]].values[0])
                    except:
                        daily = str(df[fuzzy_search('日', index_list)[0]].values[0])
                    if daily[4] == '年':
                        daily = daily.replace('年', '-').replace('月', '-').replace('日', '')
                    if daily[4] == '/':
                        daily = daily.replace('/', '-')
                    elif daily[4] != '-':
                        daily = f"{daily[:4]}-{daily[4:6]}-{daily[6:8]}"
                    try:
                        ID = remove_brackets(df[fuzzy_search('产品代码', index_list)[0]].values[0])
                    except:
                        try:
                            ID = remove_brackets(df[fuzzy_search('代码', index_list)[0]].values[0])
                        except:
                            ID = remove_brackets(df[fuzzy_search('协会备案编号', index_list)[0]].values[0])
                    UID = daily + '_' + ID
                    try:
                        name = df[fuzzy_search('基金名称', index_list)[0]].values[0]
                    except:
                        try:
                            name = df[fuzzy_search('产品名称', index_list)[0]].values[0]
                        except:
                            name = df[fuzzy_search('名称', index_list)[0]].values[0]
                    # try:
                    #     net_list = fuzzy_search('虚拟净值', index_list)
                    # except:
                    net_list = fuzzy_search('净值', index_list)
                    # print(net_list)
                    net_name = find_net_name(net_list)
                    # print(net_name[0])
                    netValue = df[net_name[0]].values[0]
                    addNetValue = df[net_name[1]].values[0]
                    a = [UID, daily, ID, name, netValue, addNetValue, file_path]
                    out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                except:
                    try:
                        if 'K_' in file_name:
                            print(file_name)
                            try:
                                df = pd.read_excel(file_path, header=2).head(1)
                            except:
                                df = pd.read_excel(file_path).head(n=1)
                        else:
                            df = pd.read_excel(file_path).head(n=1)
                        # df = pd.read_excel(file_path).head(n=1)
                        # print(df)
                        index_list = df.columns.values
                        # print(index_list)
                        daily = str(df[fuzzy_search('日期', index_list)[0]].values[0])
                        if daily[4] == '年':
                            daily = daily.replace('年', '-').replace('月', '-').replace('日', '')
                        if daily[4] == '/':
                            daily = daily.replace('/', '-')
                        elif daily[4] != '-':
                            daily = f"{daily[:4]}-{daily[4:6]}-{daily[6:8]}"
                        try:
                            ID = remove_brackets(df[fuzzy_search('产品代码', index_list)[0]].values[0])
                        except:
                            ID = remove_brackets(df[fuzzy_search('代码', index_list)[0]].values[0])
                        UID = daily + '_' + ID
                        try:
                            name = df[fuzzy_search('基金名称', index_list)[0]].values[0]
                        except:
                            try:
                                name = df[fuzzy_search('产品名称', index_list)[0]].values[0]
                            except:
                                name = df[fuzzy_search('名称', index_list)[0]].values[0]
                        # print(name)
                        net_list = fuzzy_search('净值', index_list)
                        # print(net_list)
                        net_name = find_net_name(net_list)
                        # print(net_name)
                        netValue = df[net_name[0]].values[0]
                        addNetValue = df[net_name[1]].values[0]
                        # print(UID,daily,ID,name,netValue,addNetValue)
                        a = [UID, daily, ID, name, netValue, addNetValue, file_path]
                        out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                    except :
                        try:
                            if 'e_' in file_path:
                                if '【' in file_path:
                                    tmp = D.iloc[1, :]
                                    tmp.index = D.iloc[0, :].tolist()
                                    rq0 = tmp['净值日期']
                                    rq = rq0[:4] + '-' + rq0[4:6] + '-' + rq0[-2:]
                                    UnderlyingName = tmp['基金名称']
                                    NetValue = round(float(tmp['试算前单位净值']), 4)
                                    AccNetValue = round(float(tmp['试算前累计单位净值']), 4)
                                    FundCode = tmp['基金代码']
                                    UID = rq + '_' + FundCode
                                    a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                                    out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                                else:
                                    tmp = D.iloc[1, :]
                                    rq = tmp[tmp.str.contains('-').fillna(False)].values[0][-10:]
                                    UnderlyingName = D.iloc[5, 0]
                                    NetValue = round(float(D.iloc[5, -2]), 4)
                                    AccNetValue = round(float(D.iloc[5, -1]), 4)
                                    FundCode = D.iloc[5, -3]
                                    UID = rq + '_' + FundCode
                                    a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                                    out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                        except Exception as e:
                            print(e)
                            print(file_name)
            elif '估值表' in file_name:
                try:
                    D = pd.read_excel(file_path)
                    if '波克天工量化对冲1号' in file_path:
                        FundCode = 'SB0679'
                        UnderlyingName = '波克天工量化对冲1号私募证券投资基金'
                        for ii in range(D.shape[0]):
                            if '日期' in str(D.iloc[ii, 0]):
                                rq = str(D.iloc[ii, 0])[-10:]
                            if '单位净值' == str(D.iloc[ii, 0]):  # 华泰估值表里会有 ‘累计单位净值’ ‘期初单位净值’这些东西
                                # print(str(D.iloc[ii, 0]),str(D.iloc[ii, 1]))
                                NetValue = str(D.iloc[ii, 1])
                                # print(NetValue)
                                AccNetValue = NetValue
                            # print(AccNetValue)
                        UID = rq + '_SB0679'
                        a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                        out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                    elif 'a_' in file_path or 'f_' in file_path:
                            pa1 = re.compile("^\w{3,7}_")
                            pa2 = re.compile("_\w{1,}基金_")
                            FundCode = re.findall(pa1, D.columns[0])[0][:-1]
                            UnderlyingName = re.findall(pa2, D.columns[0])[0][1:-1]
                            for ii in range(D.shape[0]):
                                if '日期' in str(D.iloc[ii, 0]):
                                    rq = D.iloc[ii, 0][-10:]
                                if '单位净值' in str(D.iloc[ii, 0]):  # 华泰估值表里会有 ‘累计单位净值’ ‘期初单位净值’这些东西
                                    # print(str(D.iloc[ii, 0]),str(D.iloc[ii, 1]))
                                    if '今日'in str(D.iloc[ii, 0]) or '单位净值' == str(D.iloc[ii, 0]):
                                        NetValue = round(float(D.iloc[ii, 1]), 4)
                                        # print(NetValue)
                                else:
                                    NetValue=0
                                if '累计单位净值' in str(D.iloc[ii, 0]):

                                    AccNetValue = round(float(D.iloc[ii, 1]), 4)
                                else:
                                    AccNetValue=NetValue
                            UID = rq + '_' + FundCode
                            a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                            out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                    elif 'b_' in file_path:
                        for ii in range(1):
                            rq = D['估值基准日'].iloc[ii]
                            if len(rq) < 4:
                                break
                            UnderlyingName = D['产品名称'].iloc[ii]
                            NetValue = round(float(D['实际净值'].iloc[ii]), 4)
                            AccNetValue = round(float(D['实际累计净值'].iloc[ii]), 4)
                            FundCode = D['产品代码'].iloc[ii]
                            UID = rq + '_' + FundCode
                            a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                        out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                    elif 'c_' in file_path:
                        if D.shape[0] > 1:
                            hang = D.shape[0] - 1
                            for ii in range(hang):  # 国军的表格最后一行是一个很大的说明
                                rq = D['净值日期'].iloc[ii]
                                UnderlyingName = D['基金名称'].iloc[ii]
                                NetValue = round(float(D['单位净值'].iloc[ii]), 4)
                                AccNetValue = round(float(D['累计单位净值'].iloc[ii]), 4)
                                FundCode = D['基金代码'].iloc[ii]
                                UID = rq + '_' + FundCode
                                a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                            out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)


                        else:
                            hang = D.shape[0]
                            for ii in range(hang):  # 国军的表格最后一行是一个很大的说明
                                rq = D['净值日期'].iloc[ii]
                                UnderlyingName = D['产品名称'].iloc[ii]
                                NetValue = round(float(D['单位净值'].iloc[ii]), 4)
                                AccNetValue = round(float(D['累计单位净值'].iloc[ii]), 4)
                                FundCode = D['产品代码'].iloc[ii]
                                UID = rq + '_' + FundCode
                                a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                            out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                    elif 'g_' in file_path:
                        pa = re.compile('_\w{3,8}_')
                        p1 = re.findall(pa, file_path)
                        FundCode = p1[0][1:-1]
                        pa2 = re.compile('_.{10}估')
                        p2 = re.findall(pa2, file_path)
                        rq = p2[0][1:-1]
                        pa3 = re.compile('__.{4,}金_')
                        p3 = re.findall(pa3, D.iloc[0, 0])
                        UnderlyingName = p3[0][3:-1]
                        for ii in range(D.shape[0]):
                            if '单位净值' in str(D.iloc[ii, 0]):
                                NetValue = round(float(D.iloc[ii, 1]), 4)
                            if '累计单位净值' in str(D.iloc[ii, 0]):
                                AccNetValue = round(float(D.iloc[ii, 1]), 4)
                        UID = rq + '_' + FundCode
                        a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                        out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                    else:
                        rq0 = str(int(D['基金净值日期'].values[0]))
                        rq = rq0[:4] + '-' + rq0[4:6] + '-' + rq0[-2:]
                        UnderlyingName = D['基金名称'].values[0]
                        NetValue = round(float(D['单位净值'].values[0]), 4)
                        AccNetValue = round(float(D['累计净值'].values[0]), 4)
                        FundCode = D['协会备案代码'].values[0]
                        UID = rq + '_' + FundCode
                        a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                        out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)

                except:
                    try:
                        match = re.search(r'[a-zA-Z]+[0-9]+', file_name)
                        if match:
                            FundCode = match.group()
                            # print(file_path)
                            D = pd.read_excel(file_path, header=1)
                            pa2 = re.compile("\w{1,}基金")
                            UnderlyingName = re.findall(pa2, D.columns[0])[0][len(FundCode):].split("_")[-1]
                            # print(UnderlyingName)
                            for ii in range(D.shape[0]):
                                # print( str(D.iloc[ii, 0]))
                                if '日期' in str(D.iloc[ii, 0]):
                                    rq = D.iloc[ii, 0].split("：")[-1]
                                    # print(rq)
                                    if rq[4] != '-':
                                        rq = f"{rq[:4]}-{rq[4:6]}-{rq[6:]}"
                                if '单位净值' in str(D.iloc[ii, 0]):
                                    if '今日' in str(D.iloc[ii, 0]) or '单位净值' == str(D.iloc[ii, 0]):
                                        NetValue = float(D.iloc[ii, 1])
                                else:
                                    NetValue=0
                                if '累计单位净值' in str(D.iloc[ii, 0]):
                                    AccNetValue = round(float(D.iloc[ii, 1]), 4)
                                else:
                                    AccNetValue=NetValue
                            UID = rq + '_' + FundCode
                            a = [UID, rq, FundCode, UnderlyingName, NetValue, AccNetValue, file_path]
                            out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                            # print(UID)
                        else:
                            print(file_name)
                    except Exception as e:
                        print(e)
                        print(file_name)
            elif '发送每日净值信息' in file_name:
                try:
                    file_name_list = file_name.split('-')
                    df = pd.read_excel(file_path, header=4, nrows=6).T
                    df.columns = df.iloc[0]
                    df_reset = df.reset_index()
                    df = df_reset.drop(0)
                    index_list = df.columns.values
                    daily = file_name_list[1].replace('年', '-').replace('月', '-').replace('日', '')
                    ID = file_name_list[0]
                    if '_' in ID:
                        ID=ID.split('_')[1]
                    UID = daily + '_' + ID
                    name = df['基金名称：'].values[0]
                    netValue = df['基金份额净值：'].values[0]
                    addNetValue = df['基金份额累计净值：'].values[0]

                    a = UID, daily, ID, name, netValue, addNetValue, file_path
                    out_df = out_df._append(pd.Series(a, index=out_df.columns), ignore_index=True)
                except Exception as e:
                    print(e)
                    print(file_name)
        # 此处将全部子单合成初始大表，并将原始数据存储下来
    out_df.to_excel('clean_ss.xlsx')
    out_df=out_df[out_df['单位净值']!=0]
    dd = out_df.drop_duplicates(subset=['UID'], keep='first')
    dd = dd[dd['UID'].apply(lambda x: len(x) <= 20)]
    dd=dd[dd['产品代码'].str.len()>5]
    dd['累计净值'] = dd['累计净值'].fillna(dd['单位净值'])
    dd['累计净值'] = pd.to_numeric(dd['累计净值'], errors='coerce')
    # 删除“累计净值”列中包含NaN（非数字）的行
    dd = dd.dropna(subset=['累计净值'])
    dd.to_excel('clean_sss2.xlsx')




if __name__ == '__main__':
    from datetime import datetime, timedelta

    # 定义日期格式
    date_format = "%d-%b-%Y"

    # 创建日期对象
    # date_obj = datetime.today()
    date_obj = datetime.today() - timedelta(days=5)
    # 将日期对象格式化为字符串
    formatted_date = date_obj.strftime(date_format)
    print(formatted_date)
    # read_csv('"6-Nov-2024"')
    read_csv(f'"{formatted_date}"')
    find_value()