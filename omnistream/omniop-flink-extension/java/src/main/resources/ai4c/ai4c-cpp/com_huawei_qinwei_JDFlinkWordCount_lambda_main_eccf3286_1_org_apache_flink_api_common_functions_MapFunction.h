#ifndef COM_HUAWEI_QINWEI_JDFLINKWORDCOUNT_LAMBDA_MAIN_ECCF3286_1_ORG_APACHE_FLINK_API_COMMON_FUNCTIONS_MAPFUNCTION_H
#define COM_HUAWEI_QINWEI_JDFLINKWORDCOUNT_LAMBDA_MAIN_ECCF3286_1_ORG_APACHE_FLINK_API_COMMON_FUNCTIONS_MAPFUNCTION_H

#include "basictypes/Arrays.h"
#include "basictypes/ClassRegistry.h"
#include "basictypes/Long.h"
#include "basictypes/String.h"
#include "basictypes/StringConstant.h"
#include "basictypes/Tuple2.h"
#include "functions/MapFunction.h"
#include "nlohmann/json.hpp"


class com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction : public MapFunction<Object> {
public:
    com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction() = default;
    ~com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction();
    Object *map(Object *obj) override;
    com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction(nlohmann::json jsonObj);
private:
    std::vector<std::string_view> split(const std::string_view &str, const std::string &pattern) {
        std::vector<std::string_view> res;
        res.reserve(2);

        const char* data = str.data();
        size_t size = str.size();

        size_t start = 0;
        size_t index = str.find(pattern);

        while (index != std::string::npos) {
            std::string_view subStr(data + start, index - start);
            res.emplace_back(subStr);
            start = index + pattern.length();
            index = str.find(pattern, start);
        }
        std::string_view subStr(data + start, size - start);
        res.emplace_back(subStr);

        return res;
    }

    String *strBuffer;
    Long *longBuffer;
    Tuple2 *result;
};

#endif
