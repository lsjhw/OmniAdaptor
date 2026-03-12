#ifndef COM_HUAWEI_QINWEI_JDFLINKWORDCOUNT_LAMBDA_MAIN_ECCF3286_2_ORG_APACHE_FLINK_API_COMMON_FUNCTIONS_MAPFUNCTION_H
#define COM_HUAWEI_QINWEI_JDFLINKWORDCOUNT_LAMBDA_MAIN_ECCF3286_2_ORG_APACHE_FLINK_API_COMMON_FUNCTIONS_MAPFUNCTION_H

#include "basictypes/ClassRegistry.h"
#include "basictypes/Object.h"
#include "basictypes/String.h"
#include "basictypes/StringBuilder.h"
#include "basictypes/StringConstant.h"
#include "basictypes/Tuple2.h"
#include "functions/MapFunction.h"
#include "nlohmann/json.hpp"


class com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction : public MapFunction<Object> {
public:
    com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction() = default;
    ~com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction();
    Object *map(Object *obj) override;
    com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction(nlohmann::json jsonObj);
private:
    std::string output;
    String* result;
};

#endif
