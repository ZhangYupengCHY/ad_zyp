"""
验证输入的数据类型是否正确
"""


class TypeVerify(object):
    """
    验证输入的数据类型是否正确
    """

    @staticmethod
    def type_valid(variables, types):
        """
        验证函数中输入参数的类型是否正确
        Parameters
        ----------
        variables : object,list of object
            需要验证的参数名
        types : type,set of type,list of type,list of set
            需要验证的参数的类型
        Returns
        -------
            bool:True,False,None
        """
        func_name = TypeVerify.type_valid.__name__
        # 输入类型验证
        if not isinstance(variables, (object, list)):
            raise_msg = TypeVerify._wrong_type_raise_msg(func_name, 'variables', f'{type(variables)}',
                                                         'str or list of str')
            raise TypeError(f'{raise_msg}')
        if not isinstance(types, (type, tuple, list)):
            raise_msg = TypeVerify._wrong_type_raise_msg(func_name, 'types', f'{type(variables)}',
                                                         'str or set of str or list of str or list of set')
            raise TypeError(f'{raise_msg}')
        # 验证的参数为单个参数,验证的数据类型为单个数据类型或是多个
        if isinstance(variables, object) and isinstance(types, (type, tuple)):
            if not isinstance(variables, types):
                raise_msg = TypeVerify._wrong_type_raise_msg(func_name, 'variables', f'{types}',f'{type(variables)}')
                raise TypeError(f'{raise_msg}')
            else:
                return True
        if isinstance(variables, list) and isinstance(types, list):
            for variable, _type in zip(variables, types):
                if not isinstance(variable, _type):
                    raise_msg = TypeVerify._wrong_type_raise_msg(func_name, 'variables', f'{_type}',
                                                                 f'{type(variables)}')
                    raise TypeError(f'{raise_msg}')
            else:
                return True

    def _wrong_type_raise_msg(func_name, variable_name, input_type, default_type):
        """
        当函数的参数的输入类型发生错误时,给的错误提示
        Parameters
        ----------
        func_name :str
            函数名
        variable_name :str
            参数类型输入错误的参数名
        input_type :str
            输入的错误的参数类型
        default_type :str
            需要输入的参数类型

        Returns
        -------
            str:
                错误提示
        """
        return f"函数:{func_name}的参数:{variable_name}的类型应该是:{default_type},而判断的类型是:{input_type}."

