//
// Created by yuhang on 2025/3/25.
//

#ifndef TIANYAN_DATABASE_H
#define TIANYAN_DATABASE_H

#include <iostream>
#include <mysql/mysql.h>
#include <fstream>
#include <string>
#include <sstream>
#include <algorithm>
#include <utility>
#include <vector>
#include <map>
#include <type_traits>
#include <random>
#include <chrono>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <filesystem>

//数据库清理输出语句缓存
inline std::vector<std::string> clean_data_message;
inline int clean_data_status;//0:未开始 1:成功 -1:失败 2:进行中
namespace yuhangle {
    struct MySqlConfig {
        std::string host;
        std::string user;
        std::string password;
        std::string database;
        unsigned int port = 3306;
    };

    class DatabaseConnection {
    public:
        explicit DatabaseConnection(MySqlConfig config) : config(std::move(config)), db(nullptr) {}

        ~DatabaseConnection() {
            if (db) {
                mysql_close(db);
            }
        }

        bool open() {
            db = mysql_init(nullptr);
            if (!db) {
                std::cerr << "无法初始化数据库连接" << std::endl;
                return false;
            }
            if (!mysql_real_connect(db,
                                    config.host.c_str(),
                                    config.user.c_str(),
                                    config.password.c_str(),
                                    config.database.c_str(),
                                    config.port,
                                    nullptr,
                                    0)) {
                std::cerr << "无法打开数据库: " << mysql_error(db) << std::endl;
                mysql_close(db);
                db = nullptr;
                return false;
            }
            mysql_set_character_set(db, "utf8mb4");
            return true;
        }

        [[nodiscard]] MYSQL* get() const { return db; }

    private:
        MySqlConfig config;
        MYSQL* db;
    };

    class ConnectionPool {
    public:
        static ConnectionPool& getInstance(const MySqlConfig& config) {
            static std::map<std::string, std::shared_ptr<ConnectionPool>> instances;
            static std::mutex instances_mutex;
            const std::string key = configKey(config);

            std::lock_guard lock(instances_mutex);
            const auto it = instances.find(key);
            if (it == instances.end()) {
                const auto pool = std::make_shared<ConnectionPool>(config);
                instances[key] = pool;
                return *pool;
            }
            return *(it->second);
        }

        explicit ConnectionPool(MySqlConfig config, const size_t pool_size = 5)
            : config(std::move(config)), pool_size(pool_size) {
            initializePool();
        }

        std::shared_ptr<DatabaseConnection> getConnection() {
            std::unique_lock lock(pool_mutex);

            // 如果连接池为空且未达到最大大小，创建新连接
            if (connections.empty() && current_size < pool_size) {
                if (auto conn = createConnection()) {
                    current_size++;
                    return conn;
                }
            }

            // 等待可用连接
            condition.wait(lock, [this]() { return !connections.empty(); });

            auto conn = connections.front();
            connections.pop();
            return conn;
        }

        void returnConnection(const std::shared_ptr<DatabaseConnection>& conn) {
            std::lock_guard lock(pool_mutex);
            connections.push(conn);
            condition.notify_one();
        }

    private:
        void initializePool() {
            for (size_t i = 0; i < pool_size; ++i) {
                if (auto conn = createConnection()) {
                    connections.push(conn);
                    current_size++;
                }
            }
        }

        std::shared_ptr<DatabaseConnection> createConnection() {
            if (auto conn = std::make_shared<DatabaseConnection>(config); conn->open()) {
                return conn;
            }
            return nullptr;
        }

        static std::string configKey(const MySqlConfig& config) {
            std::ostringstream key;
            key << config.host << ":" << config.port << ":" << config.user << ":" << config.database;
            return key.str();
        }

        MySqlConfig config;
        size_t pool_size;
        size_t current_size = 0;
        std::queue<std::shared_ptr<DatabaseConnection>> connections;
        std::mutex pool_mutex;
        std::condition_variable condition;
    };

    class Database {
    public:
        // 构造时需要指定数据库文件名
        explicit Database(MySqlConfig config) : config(std::move(config)) {
            // 预初始化连接池
            ConnectionPool::getInstance(this->config);
        }

        // 函数用于检查文件是否存在
        static bool fileExists(const std::string& filename) {
            const std::ifstream f(filename.c_str());
            return f.good();
        }

        // 初始化数据库
        [[nodiscard]] int init_database() const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();

            // 创建 LOGDATA 表，添加uuid字段作为主键
            const std::string create_logdata_table = "CREATE TABLE IF NOT EXISTS LOGDATA ("
                                            "uuid VARCHAR(36) PRIMARY KEY, "
                                            "id VARCHAR(255), "
                                            "name VARCHAR(255), "
                                            "pos_x DOUBLE, pos_y DOUBLE, pos_z DOUBLE, "
                                            "world VARCHAR(255), "
                                            "obj_id VARCHAR(255), "
                                            "obj_name VARCHAR(255), "
                                            "time BIGINT, "
                                            "type VARCHAR(255), "
                                            "data TEXT, "
                                            "status VARCHAR(64))";
            if (mysql_query(conn->get(), create_logdata_table.c_str()) != 0) {
                std::cerr << "创建 logdata 表失败: " << mysql_error(conn->get()) << std::endl;
                pool.returnConnection(conn);
                return -1;
            }

            pool.returnConnection(conn);
            return 0;
        }

        ///////////////////// 通用操作 /////////////////////

        // 通用执行 SQL 命令（用于添加、删除、修改操作）
        [[nodiscard]] int executeSQL(const std::string &sql) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();

            if (mysql_query(conn->get(), sql.c_str()) != 0) {
                std::cerr << "SQL 执行失败: " << mysql_error(conn->get()) << std::endl;
                pool.returnConnection(conn);
                return -1;
            }

            pool.returnConnection(conn);
            return 0;
        }

        // 通用查询 SQL（select查询使用回调函数返回结果为 vector<map<string, string>>）
        int querySQL(const std::string &sql, std::vector<std::map<std::string, std::string>> &result) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();

            if (mysql_query(conn->get(), sql.c_str()) != 0) {
                std::cerr << "SQL 查询失败: " << mysql_error(conn->get()) << std::endl;
                pool.returnConnection(conn);
                return -1;
            }

            MYSQL_RES* res = mysql_store_result(conn->get());
            if (!res) {
                pool.returnConnection(conn);
                return 0;
            }

            const int num_fields = mysql_num_fields(res);
            MYSQL_FIELD* fields = mysql_fetch_fields(res);

            while (MYSQL_ROW row = mysql_fetch_row(res)) {
                std::map<std::string, std::string> record;
                unsigned long* lengths = mysql_fetch_lengths(res);
                for (int i = 0; i < num_fields; i++) {
                    const std::string value = row[i] ? std::string(row[i], lengths[i]) : "NULL";
                    record[fields[i].name] = value;
                }
                result.push_back(std::move(record));
            }

            mysql_free_result(res);
            pool.returnConnection(conn);
            return 0;
        }

        [[nodiscard]] int updateSQL(const std::string &table, const std::string &set_clause, const std::string &where_clause) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();

            const std::string sql = "UPDATE " + table + " SET " + set_clause + " WHERE " + where_clause + ";";
            if (mysql_query(conn->get(), sql.c_str()) != 0) {
                std::cerr << "SQL 更新失败: " << mysql_error(conn->get()) << std::endl;
                pool.returnConnection(conn);
                return -1;
            }

            pool.returnConnection(conn);
            return 0;
        }

        // 清理数据库中超过指定时间的数据
        [[nodiscard]] bool cleanDataBase(double hours) const {
            // 设置清理数据状态为进行中
            clean_data_status = 2;
            auto start_time = std::chrono::high_resolution_clock::now();

            auto& pool = ConnectionPool::getInstance(config);
            auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            // 获取当前时间戳（秒）
            const long long currentTime = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            // 计算时间阈值（秒）
            const long long timeThreshold = currentTime - static_cast<long long>(hours * 3600);

            // 查询将要删除的记录数
            const std::string countSql = "SELECT COUNT(*) FROM LOGDATA WHERE time < " + std::to_string(timeThreshold) + ";";
            int deletedCount = 0;
            if (mysql_query(db, countSql.c_str()) != 0) {
                std::ostringstream ss;
                ss << "SQL 查询失败: " << mysql_error(db);
                clean_data_message.push_back(ss.str());
                clean_data_status = -1;
                pool.returnConnection(conn);
                return false;
            }
            MYSQL_RES* countRes = mysql_store_result(db);
            if (countRes) {
                if (MYSQL_ROW row = mysql_fetch_row(countRes)) {
                    deletedCount = row[0] ? std::stoi(row[0]) : 0;
                }
                mysql_free_result(countRes);
            }

            // 开始事务以提高性能
            if (mysql_query(db, "START TRANSACTION;") != 0) {
                std::ostringstream ss;
                ss << "Can not begin transaction: " << mysql_error(db);
                clean_data_message.push_back(ss.str());
                clean_data_status = -1;
                pool.returnConnection(conn);
                return false;
            }

            // 删除超过指定时间的数据
            const std::string deleteSql = "DELETE FROM LOGDATA WHERE time < " + std::to_string(timeThreshold) + ";";
            if (mysql_query(db, deleteSql.c_str()) != 0) {
                std::ostringstream ss;
                ss << "SQL delete failed: " << mysql_error(db);
                clean_data_message.push_back(ss.str());
                mysql_query(db, "ROLLBACK;");
                clean_data_status = -1;
                pool.returnConnection(conn);
                return false;
            }

            // 提交事务
            if (mysql_query(db, "COMMIT;") != 0) {
                std::ostringstream ss;
                ss << "Can not commit: " << mysql_error(db);
                clean_data_message.push_back(ss.str());
                mysql_query(db, "ROLLBACK;");
                clean_data_status = -1;
                pool.returnConnection(conn);
                return false;
            }

            // 整理数据库以释放空间
            mysql_query(db, "OPTIMIZE TABLE LOGDATA;");

            // 计算耗时
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            double seconds = std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();

            // 添加耗时和清理日志数信息
            clean_data_message.emplace_back("Time elapsed: ");
            clean_data_message.emplace_back(std::to_string(seconds));
            clean_data_message.emplace_back("Number of cleaned logs: ");
            clean_data_message.emplace_back(std::to_string(deletedCount));

            clean_data_status = 1;
            pool.returnConnection(conn);
            return true;
        }

        // 查找指定表中是否存在指定值
        [[nodiscard]] bool isValueExists(const std::string &tableName, const std::string &columnName, const std::string &value) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const std::string sql = "SELECT COUNT(*) FROM " + tableName + " WHERE " + columnName + " = ?;";

            MYSQL_STMT* stmt = mysql_stmt_init(db);
            if (!stmt || mysql_stmt_prepare(stmt, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 预处理失败: " << mysql_error(db) << std::endl;
                if (stmt) {
                    mysql_stmt_close(stmt);
                }
                pool.returnConnection(conn);
                return false;
            }

            // 绑定参数
            MYSQL_BIND bind{};
            unsigned long value_length = value.size();
            bind.buffer_type = MYSQL_TYPE_STRING;
            bind.buffer = const_cast<char*>(value.c_str());
            bind.buffer_length = value_length;
            bind.length = &value_length;
            if (mysql_stmt_bind_param(stmt, &bind) != 0) {
                std::cerr << "SQL 参数绑定失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return false;
            }

            // 执行查询并获取结果
            bool exists = false;
            int count = 0;
            MYSQL_BIND result_bind{};
            result_bind.buffer_type = MYSQL_TYPE_LONG;
            result_bind.buffer = &count;
            if (mysql_stmt_execute(stmt) != 0) {
                std::cerr << "SQL 查询失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return false;
            }
            if (mysql_stmt_bind_result(stmt, &result_bind) != 0) {
                std::cerr << "SQL 结果绑定失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return false;
            }
            if (mysql_stmt_fetch(stmt) == 0) {
                exists = (count > 0);
            }

            // 清理资源
            mysql_stmt_close(stmt);
            pool.returnConnection(conn);
            return exists;
        }

        // 修改指定表中的指定数据的指定值
        [[nodiscard]] bool updateValue(const std::string &tableName,
                         const std::string &targetColumn,
                         const std::string &newValue,
                         const std::string &conditionColumn,
                         const std::string &conditionValue) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const std::string sql = "UPDATE " + tableName +
                      " SET " + targetColumn + " = ?" +
                      " WHERE " + conditionColumn + " = ?;";

            MYSQL_STMT* stmt = mysql_stmt_init(db);
            if (!stmt || mysql_stmt_prepare(stmt, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 预处理失败: " << mysql_error(db) << std::endl;
                if (stmt) {
                    mysql_stmt_close(stmt);
                }
                pool.returnConnection(conn);
                return false;
            }

            // 绑定参数
            MYSQL_BIND binds[2]{};
            unsigned long new_length = newValue.size();
            unsigned long condition_length = conditionValue.size();
            binds[0].buffer_type = MYSQL_TYPE_STRING;
            binds[0].buffer = const_cast<char*>(newValue.c_str());
            binds[0].buffer_length = new_length;
            binds[0].length = &new_length;
            binds[1].buffer_type = MYSQL_TYPE_STRING;
            binds[1].buffer = const_cast<char*>(conditionValue.c_str());
            binds[1].buffer_length = condition_length;
            binds[1].length = &condition_length;
            if (mysql_stmt_bind_param(stmt, binds) != 0) {
                std::cerr << "SQL 参数绑定失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return false;
            }

            // 执行 SQL 语句
            if (mysql_stmt_execute(stmt) != 0) {
                std::cerr << "SQL 更新失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return false;
            }

            // 清理资源
            mysql_stmt_close(stmt);
            pool.returnConnection(conn);
            return true;
        }

        // 根据UUID更新指定记录的状态
        [[nodiscard]] bool updateStatusByUUID(const std::string &uuid, const std::string &newStatus) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const std::string sql = "UPDATE LOGDATA SET status = ? WHERE uuid = ?;";

            MYSQL_STMT* stmt = mysql_stmt_init(db);
            if (!stmt || mysql_stmt_prepare(stmt, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 预处理失败: " << mysql_error(db) << std::endl;
                if (stmt) {
                    mysql_stmt_close(stmt);
                }
                pool.returnConnection(conn);
                return false;
            }

            // 绑定参数
            MYSQL_BIND binds[2]{};
            unsigned long status_length = newStatus.size();
            unsigned long uuid_length = uuid.size();
            binds[0].buffer_type = MYSQL_TYPE_STRING;
            binds[0].buffer = const_cast<char*>(newStatus.c_str());
            binds[0].buffer_length = status_length;
            binds[0].length = &status_length;
            binds[1].buffer_type = MYSQL_TYPE_STRING;
            binds[1].buffer = const_cast<char*>(uuid.c_str());
            binds[1].buffer_length = uuid_length;
            binds[1].length = &uuid_length;
            if (mysql_stmt_bind_param(stmt, binds) != 0) {
                std::cerr << "SQL 参数绑定失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return false;
            }

            // 执行 SQL 语句
            if (mysql_stmt_execute(stmt) != 0) {
                std::cerr << "SQL 更新失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return false;
            }

            // 清理资源
            mysql_stmt_close(stmt);
            pool.returnConnection(conn);
            return true;
        }

        // 批量更新状态
        [[nodiscard]] bool updateStatusesByUUIDs(const std::vector<std::pair<std::string, std::string>>& uuidStatusPairs) const {
            if (uuidStatusPairs.empty()) {
                return true;
            }

            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            // 开始事务以提高性能
            if (mysql_query(db, "START TRANSACTION;") != 0) {
                std::cerr << "无法开始事务: " << mysql_error(db) << std::endl;
                pool.returnConnection(conn);
                return false;
            }

            const std::string sql = "UPDATE LOGDATA SET status = ? WHERE uuid = ?;";

            MYSQL_STMT* stmt = mysql_stmt_init(db);
            if (!stmt || mysql_stmt_prepare(stmt, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 预处理失败: " << mysql_error(db) << std::endl;
                if (stmt) {
                    mysql_stmt_close(stmt);
                }
                pool.returnConnection(conn);
                return false;
            }

            // 遍历所有UUID和状态对并更新
            for (const auto& [uuid, status] : uuidStatusPairs) {
                // 绑定参数
                MYSQL_BIND binds[2]{};
                unsigned long status_length = status.size();
                unsigned long uuid_length = uuid.size();
                binds[0].buffer_type = MYSQL_TYPE_STRING;
                binds[0].buffer = const_cast<char*>(status.c_str());
                binds[0].buffer_length = status_length;
                binds[0].length = &status_length;
                binds[1].buffer_type = MYSQL_TYPE_STRING;
                binds[1].buffer = const_cast<char*>(uuid.c_str());
                binds[1].buffer_length = uuid_length;
                binds[1].length = &uuid_length;
                if (mysql_stmt_bind_param(stmt, binds) != 0) {
                    std::cerr << "SQL 参数绑定失败: " << mysql_stmt_error(stmt) << std::endl;
                    mysql_stmt_close(stmt);
                    mysql_query(db, "ROLLBACK;");
                    pool.returnConnection(conn);
                    return false;
                }

                // 执行 SQL 语句
                if (mysql_stmt_execute(stmt) != 0) {
                    std::cerr << "SQL 更新失败: " << mysql_stmt_error(stmt) << std::endl;
                    mysql_stmt_close(stmt);
                    mysql_query(db, "ROLLBACK;");
                    pool.returnConnection(conn);
                    return false;
                }

                // 重置语句以供下次使用
                mysql_stmt_reset(stmt);
            }

            // 提交事务
            if (mysql_query(db, "COMMIT;") != 0) {
                std::cerr << "无法提交事务: " << mysql_error(db) << std::endl;
                mysql_query(db, "ROLLBACK;");
            }

            // 清理资源
            mysql_stmt_close(stmt);
            pool.returnConnection(conn);
            return true;
        }

        ///////////////////// LOGDATA 表操作 /////////////////////

        [[nodiscard]] int addLog(const std::string& uuid,
                                 const std::string& id,
                                 const std::string& name,
                                 const double pos_x, const double pos_y, const double pos_z,
                                 const std::string& world,
                                 const std::string& obj_id,
                                 const std::string& obj_name,
                                 const long long time,
                                 const std::string& type,
                                 const std::string& data,
                                 const std::string& status) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const std::string sql = "INSERT INTO LOGDATA (uuid, id, name, pos_x, pos_y, pos_z, "
                              "world, obj_id, obj_name, time, type, data, status) VALUES (?, ?, "
                              "?, ?, ?, ?, "
                              "?, ?, ?, ?, ?, ?, ?);";

            MYSQL_STMT* stmt = mysql_stmt_init(db);
            if (!stmt || mysql_stmt_prepare(stmt, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 预处理失败: " << mysql_error(db) << std::endl;
                if (stmt) {
                    mysql_stmt_close(stmt);
                }
                pool.returnConnection(conn);
                return -1;
            }

            // 绑定参数
            MYSQL_BIND binds[13]{};
            unsigned long uuid_length = uuid.size();
            unsigned long id_length = id.size();
            unsigned long name_length = name.size();
            unsigned long world_length = world.size();
            unsigned long obj_id_length = obj_id.size();
            unsigned long obj_name_length = obj_name.size();
            unsigned long type_length = type.size();
            unsigned long data_length = data.size();
            unsigned long status_length = status.size();
            long long time_value = time;
            double pos_x_value = pos_x;
            double pos_y_value = pos_y;
            double pos_z_value = pos_z;
            binds[0].buffer_type = MYSQL_TYPE_STRING;
            binds[0].buffer = const_cast<char*>(uuid.c_str());
            binds[0].buffer_length = uuid_length;
            binds[0].length = &uuid_length;
            binds[1].buffer_type = MYSQL_TYPE_STRING;
            binds[1].buffer = const_cast<char*>(id.c_str());
            binds[1].buffer_length = id_length;
            binds[1].length = &id_length;
            binds[2].buffer_type = MYSQL_TYPE_STRING;
            binds[2].buffer = const_cast<char*>(name.c_str());
            binds[2].buffer_length = name_length;
            binds[2].length = &name_length;
            binds[3].buffer_type = MYSQL_TYPE_DOUBLE;
            binds[3].buffer = &pos_x_value;
            binds[4].buffer_type = MYSQL_TYPE_DOUBLE;
            binds[4].buffer = &pos_y_value;
            binds[5].buffer_type = MYSQL_TYPE_DOUBLE;
            binds[5].buffer = &pos_z_value;
            binds[6].buffer_type = MYSQL_TYPE_STRING;
            binds[6].buffer = const_cast<char*>(world.c_str());
            binds[6].buffer_length = world_length;
            binds[6].length = &world_length;
            binds[7].buffer_type = MYSQL_TYPE_STRING;
            binds[7].buffer = const_cast<char*>(obj_id.c_str());
            binds[7].buffer_length = obj_id_length;
            binds[7].length = &obj_id_length;
            binds[8].buffer_type = MYSQL_TYPE_STRING;
            binds[8].buffer = const_cast<char*>(obj_name.c_str());
            binds[8].buffer_length = obj_name_length;
            binds[8].length = &obj_name_length;
            binds[9].buffer_type = MYSQL_TYPE_LONGLONG;
            binds[9].buffer = &time_value;
            binds[10].buffer_type = MYSQL_TYPE_STRING;
            binds[10].buffer = const_cast<char*>(type.c_str());
            binds[10].buffer_length = type_length;
            binds[10].length = &type_length;
            binds[11].buffer_type = MYSQL_TYPE_STRING;
            binds[11].buffer = const_cast<char*>(data.c_str());
            binds[11].buffer_length = data_length;
            binds[11].length = &data_length;
            binds[12].buffer_type = MYSQL_TYPE_STRING;
            binds[12].buffer = const_cast<char*>(status.c_str());
            binds[12].buffer_length = status_length;
            binds[12].length = &status_length;
            if (mysql_stmt_bind_param(stmt, binds) != 0) {
                std::cerr << "SQL 参数绑定失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return -1;
            }

            // 执行 SQL 语句
            if (mysql_stmt_execute(stmt) != 0) {
                std::cerr << "SQL 插入失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return -1;
            }

            // 清理资源
            mysql_stmt_close(stmt);
            pool.returnConnection(conn);
            return 0;
        }

        // 批量插入日志数据
        [[nodiscard]] int addLogs(const std::vector<std::tuple<std::string, std::string, std::string, double, double, double,
                                 std::string, std::string, std::string, long long, std::string, std::string, std::string>>& logs) const {
            if (logs.empty()) {
                return 0; // 空数据直接返回成功
            }

            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            // 开始事务以提高性能
            if (mysql_query(db, "START TRANSACTION;") != 0) {
                std::cerr << "无法开始事务: " << mysql_error(db) << std::endl;
                pool.returnConnection(conn);
                return -1;
            }

            const std::string sql = "INSERT INTO LOGDATA (uuid, id, name, pos_x, pos_y, pos_z, "
                              "world, obj_id, obj_name, time, type, data, status) VALUES (?, ?, "
                              "?, ?, ?, ?, "
                              "?, ?, ?, ?, ?, ?, ?);";

            MYSQL_STMT* stmt = mysql_stmt_init(db);
            if (!stmt || mysql_stmt_prepare(stmt, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 预处理失败: " << mysql_error(db) << std::endl;
                if (stmt) {
                    mysql_stmt_close(stmt);
                }
                pool.returnConnection(conn);
                return -1;
            }

            // 遍历所有日志数据并插入
            for (const auto& log : logs) {
                const auto& [uuid, id, name, pos_x, pos_y, pos_z, world, obj_id, obj_name, time, type, data, status] = log;

                // 绑定参数
                MYSQL_BIND binds[13]{};
                unsigned long uuid_length = uuid.size();
                unsigned long id_length = id.size();
                unsigned long name_length = name.size();
                unsigned long world_length = world.size();
                unsigned long obj_id_length = obj_id.size();
                unsigned long obj_name_length = obj_name.size();
                unsigned long type_length = type.size();
                unsigned long data_length = data.size();
                unsigned long status_length = status.size();
                long long time_value = time;
                double pos_x_value = pos_x;
                double pos_y_value = pos_y;
                double pos_z_value = pos_z;
                binds[0].buffer_type = MYSQL_TYPE_STRING;
                binds[0].buffer = const_cast<char*>(uuid.c_str());
                binds[0].buffer_length = uuid_length;
                binds[0].length = &uuid_length;
                binds[1].buffer_type = MYSQL_TYPE_STRING;
                binds[1].buffer = const_cast<char*>(id.c_str());
                binds[1].buffer_length = id_length;
                binds[1].length = &id_length;
                binds[2].buffer_type = MYSQL_TYPE_STRING;
                binds[2].buffer = const_cast<char*>(name.c_str());
                binds[2].buffer_length = name_length;
                binds[2].length = &name_length;
                binds[3].buffer_type = MYSQL_TYPE_DOUBLE;
                binds[3].buffer = &pos_x_value;
                binds[4].buffer_type = MYSQL_TYPE_DOUBLE;
                binds[4].buffer = &pos_y_value;
                binds[5].buffer_type = MYSQL_TYPE_DOUBLE;
                binds[5].buffer = &pos_z_value;
                binds[6].buffer_type = MYSQL_TYPE_STRING;
                binds[6].buffer = const_cast<char*>(world.c_str());
                binds[6].buffer_length = world_length;
                binds[6].length = &world_length;
                binds[7].buffer_type = MYSQL_TYPE_STRING;
                binds[7].buffer = const_cast<char*>(obj_id.c_str());
                binds[7].buffer_length = obj_id_length;
                binds[7].length = &obj_id_length;
                binds[8].buffer_type = MYSQL_TYPE_STRING;
                binds[8].buffer = const_cast<char*>(obj_name.c_str());
                binds[8].buffer_length = obj_name_length;
                binds[8].length = &obj_name_length;
                binds[9].buffer_type = MYSQL_TYPE_LONGLONG;
                binds[9].buffer = &time_value;
                binds[10].buffer_type = MYSQL_TYPE_STRING;
                binds[10].buffer = const_cast<char*>(type.c_str());
                binds[10].buffer_length = type_length;
                binds[10].length = &type_length;
                binds[11].buffer_type = MYSQL_TYPE_STRING;
                binds[11].buffer = const_cast<char*>(data.c_str());
                binds[11].buffer_length = data_length;
                binds[11].length = &data_length;
                binds[12].buffer_type = MYSQL_TYPE_STRING;
                binds[12].buffer = const_cast<char*>(status.c_str());
                binds[12].buffer_length = status_length;
                binds[12].length = &status_length;
                if (mysql_stmt_bind_param(stmt, binds) != 0) {
                    std::cerr << "SQL 参数绑定失败: " << mysql_stmt_error(stmt) << std::endl;
                    mysql_stmt_close(stmt);
                    mysql_query(db, "ROLLBACK;");
                    pool.returnConnection(conn);
                    return -1;
                }

                // 执行 SQL 语句
                if (mysql_stmt_execute(stmt) != 0) {
                    std::cerr << "SQL 插入失败: " << mysql_stmt_error(stmt) << std::endl;
                    mysql_stmt_close(stmt);
                    mysql_query(db, "ROLLBACK;");
                    pool.returnConnection(conn);
                    return -1;
                }

                // 重置语句以供下次使用
                mysql_stmt_reset(stmt);
            }

            // 提交事务
            if (mysql_query(db, "COMMIT;") != 0) {
                std::cerr << "无法提交事务: " << mysql_error(db) << std::endl;
                mysql_query(db, "ROLLBACK;");
            }

            // 清理资源
            mysql_stmt_close(stmt);
            pool.returnConnection(conn);
            return 0;
        }

        int getAllLog(std::vector<std::map<std::string, std::string>> &result) const {
            // 获取所有数据
            const std::string sql = "SELECT * FROM LOGDATA;";

            // 调用 querySQL 函数执行查询，并将结果存储到 result 中
            return querySQL(sql, result);
        }

        int searchLog(std::vector<std::map<std::string, std::string>> &result,
                      const std::pair<std::string, double>& searchCriteria) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            // 获取当前时间戳（秒）
            const long long currentTime = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            // 计算时间范围（秒），支持小数小时（如0.5表示半小时）
            const long long timeThreshold = currentTime - static_cast<long long>(searchCriteria.second * 3600);

            // 使用参数化查询防止SQL注入
            const std::string sql = "SELECT * FROM LOGDATA WHERE (name LIKE ? OR type LIKE ? OR data LIKE ?) AND time >= ? LIMIT 10000;";

            MYSQL_STMT* stmt = mysql_stmt_init(db);
            if (!stmt || mysql_stmt_prepare(stmt, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 预处理失败: " << mysql_error(db) << std::endl;
                if (stmt) {
                    mysql_stmt_close(stmt);
                }
                pool.returnConnection(conn);
                return -1;
            }

            // 构造搜索关键词（添加通配符）
            const std::string searchPattern = "%" + searchCriteria.first + "%";

            // 绑定参数
            MYSQL_BIND binds[4]{};
            unsigned long pattern_length = searchPattern.size();
            long long time_value = timeThreshold;
            binds[0].buffer_type = MYSQL_TYPE_STRING;
            binds[0].buffer = const_cast<char*>(searchPattern.c_str());
            binds[0].buffer_length = pattern_length;
            binds[0].length = &pattern_length;
            binds[1].buffer_type = MYSQL_TYPE_STRING;
            binds[1].buffer = const_cast<char*>(searchPattern.c_str());
            binds[1].buffer_length = pattern_length;
            binds[1].length = &pattern_length;
            binds[2].buffer_type = MYSQL_TYPE_STRING;
            binds[2].buffer = const_cast<char*>(searchPattern.c_str());
            binds[2].buffer_length = pattern_length;
            binds[2].length = &pattern_length;
            binds[3].buffer_type = MYSQL_TYPE_LONGLONG;
            binds[3].buffer = &time_value;
            if (mysql_stmt_bind_param(stmt, binds) != 0) {
                std::cerr << "SQL 参数绑定失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return -1;
            }

            // 执行查询并处理结果
            if (mysql_stmt_execute(stmt) != 0) {
                std::cerr << "SQL 查询失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return -1;
            }

            if (!fetchStatementResults(stmt, result)) {
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return -1;
            }

            mysql_stmt_close(stmt);
            pool.returnConnection(conn);
            return 0;
        }

        // 新增带坐标和世界过滤的搜索函数
        int searchLog(std::vector<std::map<std::string, std::string>> &result,
                      const std::pair<std::string, double>& searchCriteria,
                      const double x, const double y, const double z, const double r, const std::string& world, bool if_max = false) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            // 获取当前时间戳（秒）
            const long long currentTime = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            // 计算时间范围（秒），支持小数小时（如0.5表示半小时）
            const long long timeThreshold = currentTime - static_cast<long long>(searchCriteria.second * 3600);

            // 使用参数化查询防止SQL注入
            std::string sql;
            if (if_max) {
                sql = "SELECT * FROM LOGDATA WHERE "
                      "(name LIKE ? OR type LIKE ? OR data LIKE ?) AND time >= ? "
                      "AND world = ? "
                      "AND ((pos_x - ?)*(pos_x - ?) + (pos_y - ?)*(pos_y - ?) + (pos_z - ?)*(pos_z - ?)) <= ?;";
            } else {
                sql = "SELECT * FROM LOGDATA WHERE "
                           "(name LIKE ? OR type LIKE ? OR data LIKE ?) AND time >= ? "
                           "AND world = ? "
                           "AND ((pos_x - ?)*(pos_x - ?) + (pos_y - ?)*(pos_y - ?) + (pos_z - ?)*(pos_z - ?)) <= ? "
                           "LIMIT 10000;";
            }

            MYSQL_STMT* stmt = mysql_stmt_init(db);
            if (!stmt || mysql_stmt_prepare(stmt, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 预处理失败: " << mysql_error(db) << std::endl;
                if (stmt) {
                    mysql_stmt_close(stmt);
                }
                pool.returnConnection(conn);
                return -1;
            }

            // 构造搜索关键词（添加通配符）
            const std::string searchPattern = "%" + searchCriteria.first + "%";

            // 绑定参数
            MYSQL_BIND binds[12]{};
            unsigned long pattern_length = searchPattern.size();
            unsigned long world_length = world.size();
            long long time_value = timeThreshold;
            double x_value = x;
            double y_value = y;
            double z_value = z;
            double r_value = r * r;
            binds[0].buffer_type = MYSQL_TYPE_STRING;
            binds[0].buffer = const_cast<char*>(searchPattern.c_str());
            binds[0].buffer_length = pattern_length;
            binds[0].length = &pattern_length;
            binds[1].buffer_type = MYSQL_TYPE_STRING;
            binds[1].buffer = const_cast<char*>(searchPattern.c_str());
            binds[1].buffer_length = pattern_length;
            binds[1].length = &pattern_length;
            binds[2].buffer_type = MYSQL_TYPE_STRING;
            binds[2].buffer = const_cast<char*>(searchPattern.c_str());
            binds[2].buffer_length = pattern_length;
            binds[2].length = &pattern_length;
            binds[3].buffer_type = MYSQL_TYPE_LONGLONG;
            binds[3].buffer = &time_value;
            binds[4].buffer_type = MYSQL_TYPE_STRING;
            binds[4].buffer = const_cast<char*>(world.c_str());
            binds[4].buffer_length = world_length;
            binds[4].length = &world_length;
            binds[5].buffer_type = MYSQL_TYPE_DOUBLE;
            binds[5].buffer = &x_value;
            binds[6].buffer_type = MYSQL_TYPE_DOUBLE;
            binds[6].buffer = &x_value;
            binds[7].buffer_type = MYSQL_TYPE_DOUBLE;
            binds[7].buffer = &y_value;
            binds[8].buffer_type = MYSQL_TYPE_DOUBLE;
            binds[8].buffer = &y_value;
            binds[9].buffer_type = MYSQL_TYPE_DOUBLE;
            binds[9].buffer = &z_value;
            binds[10].buffer_type = MYSQL_TYPE_DOUBLE;
            binds[10].buffer = &z_value;
            binds[11].buffer_type = MYSQL_TYPE_DOUBLE;
            binds[11].buffer = &r_value;
            if (mysql_stmt_bind_param(stmt, binds) != 0) {
                std::cerr << "SQL 参数绑定失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return -1;
            }

            // 执行查询并处理结果
            if (mysql_stmt_execute(stmt) != 0) {
                std::cerr << "SQL 查询失败: " << mysql_stmt_error(stmt) << std::endl;
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return -1;
            }

            if (!fetchStatementResults(stmt, result)) {
                mysql_stmt_close(stmt);
                pool.returnConnection(conn);
                return -1;
            }

            mysql_stmt_close(stmt);
            pool.returnConnection(conn);
            return 0;
        }

        //数据库工具

        //将逗号字符串分割为vector
        static std::vector<std::string> splitString(const std::string& input) {
            std::vector<std::string> result; // 用于存储分割后的结果
            std::string current;             // 当前正在构建的子字符串
            bool inBraces = false;           // 标记是否在 {} 内部
            bool inBrackets = false;         // 标记是否在 [] 内部

            for (const char ch : input) {
                if (ch == '{') {
                    // 遇到左大括号，标记进入 {} 内部
                    inBraces = true;
                    current += ch; // 将 { 添加到当前字符串
                } else if (ch == '}') {
                    // 遇到右大括号，标记离开 {} 内部
                    inBraces = false;
                    current += ch; // 将 } 添加到当前字符串
                } else if (ch == '[') {
                    // 遇到左方括号，标记进入 [] 内部
                    inBrackets = true;
                    current += ch; // 将 [ 添加到当前字符串
                } else if (ch == ']') {
                    // 遇到右方括号，标记离开 [] 内部
                    inBrackets = false;
                    current += ch; // 将 ] 添加到当前字符串
                } else if (ch == ',' && !inBraces && !inBrackets) {
                    // 遇到逗号且不在 {} 和 [] 内部时，分割字符串
                    if (!current.empty()) {
                        result.push_back(current);
                        current.clear();
                    }
                } else {
                    // 其他情况，将字符添加到当前字符串
                    current += ch;
                }
            }

            // 处理最后一个子字符串（如果非空）
            if (!current.empty()) {
                result.push_back(current);
            }

            return result;
        }

        //将数字字符逗号分割为vector
        static std::vector<int> splitStringInt(const std::string& input) {
            std::vector<int> result; // 用于存储分割后的结果
            std::stringstream ss(input);     // 使用 string stream 处理输入字符串
            std::string item;

            // 按逗号分割字符串
            while (std::getline(ss, item, ',')) {
                if (!item.empty()) {         // 如果分割出的元素非空，则添加到结果中
                    std::stringstream sss(item);
                    int groupid = 0;
                    if (!(sss >> groupid)) {
                        return {}; // 如果解析失败，返回空
                    }
                    result.push_back(groupid);
                }
            }

            // 如果没有逗号且结果为空则返回空
            if (result.empty()) {
                return {};
            }

            return result;
        }

        //将vector(string)通过逗号分隔改为字符串
        static std::string vectorToString(const std::vector<std::string>& vec) {
            if (vec.empty()) {
                return ""; // 如果向量为空，返回空字符串
            }

            std::ostringstream oss; // 使用字符串流拼接结果
            for (size_t i = 0; i < vec.size(); ++i) {
                oss << vec[i]; // 添加当前元素
                if (i != vec.size() - 1 && !(vec[i].empty())) { // 如果不是最后一个元素且不为空，添加逗号
                    oss << ",";
                }
            }

            return oss.str(); // 返回拼接后的字符串
        }

        //将vector(int)通过逗号分隔改为字符串
        static std::string IntVectorToString(const std::vector<int>& vec) {
            if (vec.empty()) {
                return ""; // 如果向量为空，返回空字符串
            }

            std::ostringstream oss; // 使用字符串流拼接结果
            for (size_t i = 0; i < vec.size(); ++i) {
                oss << vec[i]; // 添加当前元素
                if (i != vec.size() - 1) { // 如果不是最后一个元素，添加逗号
                    oss << ",";
                }
            }

            return oss.str(); // 返回拼接后的字符串
        }

        //字符串改整数
        static int stringToInt(const std::string& str) {
            try {
                // 尝试将字符串转换为整数
                return std::stoi(str);
            } catch (const std::invalid_argument&) {
                // 捕获无效参数异常（例如无法解析为整数）
                return 0;
            } catch (const std::out_of_range&) {
                // 捕获超出范围异常（例如数值过大或过小）
                return 0;
            }
        }

        // 生成一个符合 RFC 4122 标准的 UUID v4
        static std::string generate_uuid_v4() {
            static thread_local std::mt19937 gen{std::random_device{}()};

            std::uniform_int_distribution<int> dis(0, 15);

            std::stringstream ss;
            ss << std::hex; // 设置为十六进制输出

            for (int i = 0; i < 8; ++i) ss << dis(gen);
            ss << "-";
            for (int i = 0; i < 4; ++i) ss << dis(gen);
            ss << "-";

            ss << "4"; // 版本号为 4
            for (int i = 0; i < 3; ++i) ss << dis(gen);
            ss << "-";

            ss << (dis(gen) & 0x3 | 0x8);
            for (int i = 0; i < 3; ++i) ss << dis(gen);
            ss << "-";

            for (int i = 0; i < 12; ++i) ss << dis(gen);

            return ss.str();
        }

    private:
        static bool fetchStatementResults(MYSQL_STMT* stmt, std::vector<std::map<std::string, std::string>>& result) {
            MYSQL_RES* meta = mysql_stmt_result_metadata(stmt);
            if (!meta) {
                return true;
            }
            const int column_count = mysql_num_fields(meta);
            std::vector<MYSQL_BIND> binds(column_count);
            std::vector<std::vector<char>> buffers(column_count);
            std::vector<unsigned long> lengths(column_count);
            using MySqlNullFlag = std::remove_pointer_t<decltype(MYSQL_BIND{}.is_null)>;
            std::vector<MySqlNullFlag> is_null(column_count);
            MYSQL_FIELD* fields = mysql_fetch_fields(meta);

            for (int i = 0; i < column_count; ++i) {
                unsigned long size = std::max(fields[i].max_length, fields[i].length);
                if (size == 0) {
                    size = 1024;
                }
                buffers[i].resize(size + 1);
                binds[i].buffer_type = MYSQL_TYPE_STRING;
                binds[i].buffer = buffers[i].data();
                binds[i].buffer_length = buffers[i].size();
                binds[i].length = &lengths[i];
                binds[i].is_null = &is_null[i];
            }

            if (mysql_stmt_bind_result(stmt, binds.data()) != 0) {
                mysql_free_result(meta);
                return false;
            }
            my_bool update_max_length = 1;
            mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &update_max_length);
            if (mysql_stmt_store_result(stmt) != 0) {
                mysql_free_result(meta);
                return false;
            }

            int status = 0;
            while ((status = mysql_stmt_fetch(stmt)) == 0 || status == MYSQL_DATA_TRUNCATED) {
                std::map<std::string, std::string> row;
                for (int i = 0; i < column_count; ++i) {
                    if (is_null[i]) {
                        row[fields[i].name] = "NULL";
                    } else {
                        row[fields[i].name] = std::string(buffers[i].data(), lengths[i]);
                    }
                }
                result.push_back(std::move(row));
            }

            mysql_free_result(meta);
            return status == 0 || status == MYSQL_NO_DATA;
        }

        MySqlConfig config;
    };
}
#endif // TIANYAN_DATABASE_H
