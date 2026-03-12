#ifndef COM_HUAWEI_QINWEI_JDFLINKWORDCOUNT_LAMBDA_MAIN_B9058687_1_ORG_APACHE_FLINK_API_JAVA_FUNCTIONS_KEYSELECTOR_H
#define COM_HUAWEI_QINWEI_JDFLINKWORDCOUNT_LAMBDA_MAIN_B9058687_1_ORG_APACHE_FLINK_API_JAVA_FUNCTIONS_KEYSELECTOR_H

#include "basictypes/ClassRegistry.h"
#include "basictypes/Object.h"
#include "basictypes/String.h"
#include "basictypes/StringConstant.h"
#include "basictypes/Tuple2.h"
#include "functions/KeySelect.h"
#include "nlohmann/json.hpp"


class com_huawei_qinwei_JDFlinkWordCount_lambda_main_b9058687_1_org_apache_flink_api_java_functions_KeySelector : public KeySelect<Object> {
public:
    com_huawei_qinwei_JDFlinkWordCount_lambda_main_b9058687_1_org_apache_flink_api_java_functions_KeySelector() = default;
    Object *getKey(Object *obj) override;
    com_huawei_qinwei_JDFlinkWordCount_lambda_main_b9058687_1_org_apache_flink_api_java_functions_KeySelector(nlohmann::json jsonObj);
};

#endif
