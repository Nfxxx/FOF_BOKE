def run_pipeline(pipeline, mars=None):
    """运行pipeline

    :param pipeline（class）: 事例化的Pipeline的对象，详情参考Pipeline的文档
    :param mars(bool):是否使用mars并发进行加速，默认为不使用，此时是直接python实现
    :return:
        dict：Pipeline运行对结果返回
    """
    columns = pipeline.columns
    outcome = {}
    if mars is None or mars is False:
        for keys, func in columns.itesm():
            return_list = []
            for key, args in func.items():
                for total_params in args:
                    result = key(**total_params)
                    return_list.append(result)
            outcome.update([keys, return_list])
        return outcome
    elif mars is True:
        import mars.remote as mr
        for keys, func in columns.itesm():
            return_list = []
            for key, args in func.items():
                for total_params in args:
                    return_list.append(mr.spawn(key, args=tuple(total_params.values())))
            outcome.update([keys, return_list])
        result_value = (mr.spawn(mars_return, args=outcome)).execute().fetch()
        return result_value


def mars_return(mr):
    return mr
