//
// Created by yuhang on 2025/11/6.
//

#ifndef TIANYAN_GLOBAL_H
#define TIANYAN_GLOBAL_H
#include "DataBase.hpp"
#include "TianyanCore.h"
#include "translate.h"
#include "endstone/endstone.hpp"
#include <nlohmann/json.hpp>
using namespace nlohmann;
using namespace std;
//文件目录
inline string dataPath = "plugins/tianyan_data";
inline string dbPath = "plugins/tianyan_data/ty_data.db";
inline string config_path = "plugins/tianyan_data/config.json";
//配置变量
inline int max_message_in_10s;
inline int max_command_in_10s;
inline vector<string> no_log_mobs;
//日志缓存
inline vector<TianyanCore::LogData> logDataCache;
//语言
inline translate Tran;
//初始化其它实例
inline DataBase Database(dbPath);
inline TianyanCore tyCore(Database);

// 存储每个玩家的上次触发时间
inline std::unordered_map<string, std::chrono::steady_clock::time_point> lastTriggerTime;
//任务
inline shared_ptr<endstone::Task> auto_write_task;
// 回溯状态缓存 - 存储需要标记为"reverted"的日志UUID和状态
inline vector<pair<string, string>> revertStatusCache;
#endif //TIANYAN_GLOBAL_H