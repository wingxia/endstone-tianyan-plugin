//
// Created by yuhang on 2025/3/25.
//

#ifndef TIANYAN_DATABASE_H
#define TIANYAN_DATABASE_H

#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#ifdef _WIN32
#include <mysql.h>
#else
#include <mysql/mysql.h>
#endif
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

//数据库清理输出语句缓存
inline std::vector<std::string> clean_data_message;
inline int clean_data_status;//0:未开始 1:成功 -1:失败 2:进行中

namespace yuhangle {
    struct MysqlConfig {
        std::string host;
        std::string user;
        std::string password;
        std::string database;
        unsigned int port;
    };

    class DatabaseConnection {
    public:
        explicit DatabaseConnection(MysqlConfig config) : config(std::move(config)), db(nullptr) {}

        ~DatabaseConnection() {
            if (db) {
                mysql_close(db);
            }
        }

        bool open() {
            db = mysql_init(nullptr);
            if (!db) {
                return false;
            }
            mysql_options(db, MYSQL_SET_CHARSET_NAME, "utf8mb4");
            if (!mysql_real_connect(db,
                                    config.host.c_str(),
                                    config.user.c_str(),
                                    config.password.c_str(),
                                    nullptr,
                                    config.port,
                                    nullptr,
                                    0)) {
                return false;
            }

            const std::string create_db = "CREATE DATABASE IF NOT EXISTS `" + config.database +
                                           "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;";
            if (mysql_real_query(db, create_db.c_str(), create_db.size()) != 0) {
                return false;
            }

            if (mysql_select_db(db, config.database.c_str()) != 0) {
                return false;
            }

            return true;
        }

        [[nodiscard]] MYSQL* get() const { return db; }

    private:
        MysqlConfig config;
        MYSQL* db;
    };

    class ConnectionPool {
    public:
        static ConnectionPool& getInstance(const MysqlConfig& config) {
            static std::map<std::string, std::shared_ptr<ConnectionPool>> instances;
            static std::mutex instances_mutex;

            std::lock_guard lock(instances_mutex);
            const std::string key = configKey(config);
            const auto it = instances.find(key);
            if (it == instances.end()) {
                const auto pool = std::make_shared<ConnectionPool>(config);
                instances[key] = pool;
                return *pool;
            }
            return *(it->second);
        }

        explicit ConnectionPool(MysqlConfig config, const size_t pool_size = 5)
            : config(std::move(config)), pool_size(pool_size) {
            initializePool();
        }

        std::shared_ptr<DatabaseConnection> getConnection() {
            std::unique_lock lock(pool_mutex);

            if (connections.empty() && current_size < pool_size) {
                if (auto conn = createConnection()) {
                    current_size++;
                    return conn;
                }
            }

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
        static std::string configKey(const MysqlConfig& config) {
            return config.host + "|" + config.user + "|" + config.password + "|" + config.database + "|" + std::to_string(config.port);
        }

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

        MysqlConfig config;
        size_t pool_size;
        size_t current_size = 0;
        std::queue<std::shared_ptr<DatabaseConnection>> connections;
        std::mutex pool_mutex;
        std::condition_variable condition;
    };

    class Database {
    public:
        explicit Database(MysqlConfig config) : config(std::move(config)) {
            ConnectionPool::getInstance(this->config);
        }

        static bool fileExists(const std::string& filename) {
            const std::ifstream f(filename.c_str());
            return f.good();
        }

        [[nodiscard]] int init_database() const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const std::string create_logdata_table =
                "CREATE TABLE IF NOT EXISTS LOGDATA ("
                "uuid VARCHAR(64) PRIMARY KEY, "
                "id TEXT, "
                "name TEXT, "
                "pos_x DOUBLE, pos_y DOUBLE, pos_z DOUBLE, "
                "world TEXT, "
                "obj_id TEXT, "
                "obj_name TEXT, "
                "time BIGINT, "
                "type TEXT, "
                "data TEXT, "
                "status TEXT)";

            if (mysql_real_query(db, create_logdata_table.c_str(), create_logdata_table.size()) != 0) {
                std::cerr << "创建 logdata 表失败: " << mysql_error(db) << std::endl;
                pool.returnConnection(conn);
                return mysql_errno(db);
            }

            pool.returnConnection(conn);
            return 0;
        }

        [[nodiscard]] int executeSQL(const std::string &sql) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const int rc = mysql_real_query(db, sql.c_str(), sql.size());
            if (rc != 0) {
                std::cerr << "SQL 执行失败: " << mysql_error(db) << std::endl;
            }

            pool.returnConnection(conn);
            return rc;
        }

        static std::string escapeString(MYSQL* db, const std::string& input) {
            std::string escaped;
            escaped.resize(input.size() * 2 + 1);
            const auto len = mysql_real_escape_string(db, escaped.data(), input.c_str(), input.size());
            escaped.resize(len);
            return escaped;
        }

        int querySQL(const std::string &sql, std::vector<std::map<std::string, std::string>> &result) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const int rc = mysql_real_query(db, sql.c_str(), sql.size());
            if (rc != 0) {
                std::cerr << "SQL 查询失败: " << mysql_error(db) << std::endl;
                pool.returnConnection(conn);
                return rc;
            }

            MYSQL_RES* res = mysql_store_result(db);
            if (!res) {
                if (mysql_field_count(db) != 0) {
                    std::cerr << "SQL 查询结果失败: " << mysql_error(db) << std::endl;
                    pool.returnConnection(conn);
                    return mysql_errno(db);
                }
                pool.returnConnection(conn);
                return 0;
            }

            const int num_fields = mysql_num_fields(res);
            MYSQL_FIELD* fields = mysql_fetch_fields(res);
            MYSQL_ROW row;
            while ((row = mysql_fetch_row(res))) {
                unsigned long* lengths = mysql_fetch_lengths(res);
                std::map<std::string, std::string> row_data;
                for (int i = 0; i < num_fields; i++) {
                    const char* col_name = fields[i].name;
                    if (row[i]) {
                        row_data[col_name] = std::string(row[i], lengths[i]);
                    } else {
                        row_data[col_name] = "NULL";
                    }
                }
                result.push_back(std::move(row_data));
            }

            mysql_free_result(res);
            pool.returnConnection(conn);
            return 0;
        }

        [[nodiscard]] int updateSQL(const std::string &table, const std::string &set_clause, const std::string &where_clause) const {
            const std::string sql = "UPDATE " + table + " SET " + set_clause + " WHERE " + where_clause + ";";
            return executeSQL(sql);
        }

        [[nodiscard]] bool cleanDataBase(double hours) const {
            clean_data_status = 2;
            auto start_time = std::chrono::high_resolution_clock::now();

            auto& pool = ConnectionPool::getInstance(config);
            auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const long long currentTime = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            const long long timeThreshold = currentTime - static_cast<long long>(hours * 3600);

            const std::string countSql = "SELECT COUNT(*) FROM LOGDATA WHERE time < " + std::to_string(timeThreshold) + ";";
            if (mysql_real_query(db, countSql.c_str(), countSql.size()) != 0) {
                clean_data_message.push_back("SQL 预处理失败: " + std::string(mysql_error(db)));
                clean_data_status = -1;
                pool.returnConnection(conn);
                return false;
            }

            MYSQL_RES* countRes = mysql_store_result(db);
            int deletedCount = 0;
            if (countRes) {
                if (MYSQL_ROW row = mysql_fetch_row(countRes)) {
                    deletedCount = row[0] ? std::stoi(row[0]) : 0;
                }
                mysql_free_result(countRes);
            }

            if (mysql_real_query(db, "START TRANSACTION;", 18) != 0) {
                clean_data_message.push_back("Can not begin transaction: " + std::string(mysql_error(db)));
                clean_data_status = -1;
                pool.returnConnection(conn);
                return false;
            }

            const std::string deleteSql = "DELETE FROM LOGDATA WHERE time < " + std::to_string(timeThreshold) + ";";
            if (mysql_real_query(db, deleteSql.c_str(), deleteSql.size()) != 0) {
                clean_data_message.push_back("SQL delete failed: " + std::string(mysql_error(db)));
                mysql_real_query(db, "ROLLBACK;", 9);
                clean_data_status = -1;
                pool.returnConnection(conn);
                return false;
            }

            if (mysql_real_query(db, "COMMIT;", 7) != 0) {
                clean_data_message.push_back("Can not commit: " + std::string(mysql_error(db)));
                mysql_real_query(db, "ROLLBACK;", 9);
                clean_data_status = -1;
                pool.returnConnection(conn);
                return false;
            }

            const std::string optimizeSql = "OPTIMIZE TABLE LOGDATA;";
            mysql_real_query(db, optimizeSql.c_str(), optimizeSql.size());

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            double seconds = std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();

            clean_data_message.emplace_back("Time elapsed: ");
            clean_data_message.emplace_back(std::to_string(seconds));
            clean_data_message.emplace_back("Number of cleaned logs: ");
            clean_data_message.emplace_back(std::to_string(deletedCount));

            clean_data_status = 1;
            pool.returnConnection(conn);
            return true;
        }

        [[nodiscard]] bool isValueExists(const std::string &tableName, const std::string &columnName, const std::string &value) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const std::string escapedValue = escapeString(db, value);
            const std::string sql = "SELECT COUNT(*) FROM " + tableName + " WHERE " + columnName + " = '" + escapedValue + "';";

            if (mysql_real_query(db, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 预处理失败: " << mysql_error(db) << std::endl;
                pool.returnConnection(conn);
                return false;
            }

            MYSQL_RES* res = mysql_store_result(db);
            bool exists = false;
            if (res) {
                if (MYSQL_ROW row = mysql_fetch_row(res)) {
                    const int count = row[0] ? std::stoi(row[0]) : 0;
                    exists = (count > 0);
                }
                mysql_free_result(res);
            }

            pool.returnConnection(conn);
            return exists;
        }

        [[nodiscard]] bool updateValue(const std::string &tableName,
                         const std::string &targetColumn,
                         const std::string &newValue,
                         const std::string &conditionColumn,
                         const std::string &conditionValue) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const std::string sql = "UPDATE " + tableName +
                      " SET " + targetColumn + " = '" + escapeString(db, newValue) +
                      "' WHERE " + conditionColumn + " = '" + escapeString(db, conditionValue) + "';";

            if (mysql_real_query(db, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 更新失败: " << mysql_error(db) << std::endl;
                pool.returnConnection(conn);
                return false;
            }

            pool.returnConnection(conn);
            return true;
        }

        [[nodiscard]] bool updateStatusByUUID(const std::string &uuid, const std::string &newStatus) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const std::string sql = "UPDATE LOGDATA SET status = '" + escapeString(db, newStatus) +
                                    "' WHERE uuid = '" + escapeString(db, uuid) + "';";

            if (mysql_real_query(db, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 更新失败: " << mysql_error(db) << std::endl;
                pool.returnConnection(conn);
                return false;
            }

            pool.returnConnection(conn);
            return true;
        }

        [[nodiscard]] bool updateStatusesByUUIDs(const std::vector<std::pair<std::string, std::string>>& uuidStatusPairs) const {
            if (uuidStatusPairs.empty()) {
                return true;
            }

            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            if (mysql_real_query(db, "START TRANSACTION;", 18) != 0) {
                std::cerr << "无法开始事务: " << mysql_error(db) << std::endl;
                pool.returnConnection(conn);
                return false;
            }

            for (const auto& [uuid, status] : uuidStatusPairs) {
                const std::string sql = "UPDATE LOGDATA SET status = '" + escapeString(db, status) +
                                        "' WHERE uuid = '" + escapeString(db, uuid) + "';";

                if (mysql_real_query(db, sql.c_str(), sql.size()) != 0) {
                    std::cerr << "SQL 更新失败: " << mysql_error(db) << std::endl;
                    mysql_real_query(db, "ROLLBACK;", 9);
                    pool.returnConnection(conn);
                    return false;
                }
            }

            if (mysql_real_query(db, "COMMIT;", 7) != 0) {
                std::cerr << "无法提交事务: " << mysql_error(db) << std::endl;
                mysql_real_query(db, "ROLLBACK;", 9);
            }

            pool.returnConnection(conn);
            return true;
        }

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
                              "world, obj_id, obj_name, time, type, data, status) VALUES ('" +
                              escapeString(db, uuid) + "', '" + escapeString(db, id) + "', '" + escapeString(db, name) +
                              "', " + std::to_string(pos_x) + ", " + std::to_string(pos_y) + ", " + std::to_string(pos_z) +
                              ", '" + escapeString(db, world) + "', '" + escapeString(db, obj_id) + "', '" + escapeString(db, obj_name) +
                              "', " + std::to_string(time) + ", '" + escapeString(db, type) + "', '" + escapeString(db, data) +
                              "', '" + escapeString(db, status) + "');";

            if (mysql_real_query(db, sql.c_str(), sql.size()) != 0) {
                std::cerr << "SQL 插入失败: " << mysql_error(db) << std::endl;
                pool.returnConnection(conn);
                return mysql_errno(db);
            }

            pool.returnConnection(conn);
            return 0;
        }

        [[nodiscard]] int addLogs(const std::vector<std::tuple<std::string, std::string, std::string, double, double, double,
                                 std::string, std::string, std::string, long long, std::string, std::string, std::string>>& logs) const {
            if (logs.empty()) {
                return 0;
            }

            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            if (mysql_real_query(db, "START TRANSACTION;", 18) != 0) {
                std::cerr << "无法开始事务: " << mysql_error(db) << std::endl;
                pool.returnConnection(conn);
                return mysql_errno(db);
            }

            for (const auto& log : logs) {
                const auto& [uuid, id, name, pos_x, pos_y, pos_z, world, obj_id, obj_name, time, type, data, status] = log;
                const std::string sql = "INSERT INTO LOGDATA (uuid, id, name, pos_x, pos_y, pos_z, "
                                  "world, obj_id, obj_name, time, type, data, status) VALUES ('" +
                                  escapeString(db, uuid) + "', '" + escapeString(db, id) + "', '" + escapeString(db, name) +
                                  "', " + std::to_string(pos_x) + ", " + std::to_string(pos_y) + ", " + std::to_string(pos_z) +
                                  ", '" + escapeString(db, world) + "', '" + escapeString(db, obj_id) + "', '" + escapeString(db, obj_name) +
                                  "', " + std::to_string(time) + ", '" + escapeString(db, type) + "', '" + escapeString(db, data) +
                                  "', '" + escapeString(db, status) + "');";

                if (mysql_real_query(db, sql.c_str(), sql.size()) != 0) {
                    std::cerr << "SQL 插入失败: " << mysql_error(db) << std::endl;
                    mysql_real_query(db, "ROLLBACK;", 9);
                    pool.returnConnection(conn);
                    return mysql_errno(db);
                }
            }

            if (mysql_real_query(db, "COMMIT;", 7) != 0) {
                std::cerr << "无法提交事务: " << mysql_error(db) << std::endl;
                mysql_real_query(db, "ROLLBACK;", 9);
            }

            pool.returnConnection(conn);
            return 0;
        }

        int getAllLog(std::vector<std::map<std::string, std::string>> &result) const {
            const std::string sql = "SELECT * FROM LOGDATA;";
            return querySQL(sql, result);
        }

        int searchLog(std::vector<std::map<std::string, std::string>> &result,
                      const std::pair<std::string, double>& searchCriteria) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const long long currentTime = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            const long long timeThreshold = currentTime - static_cast<long long>(searchCriteria.second * 3600);

            const std::string searchPattern = "'%" + escapeString(db, searchCriteria.first) + "%'";
            const std::string sql = "SELECT * FROM LOGDATA WHERE (name LIKE " + searchPattern +
                                    " OR type LIKE " + searchPattern + " OR data LIKE " + searchPattern +
                                    ") AND time >= " + std::to_string(timeThreshold) + " LIMIT 10000;";

            pool.returnConnection(conn);
            return querySQL(sql, result);
        }

        int searchLog(std::vector<std::map<std::string, std::string>> &result,
                      const std::pair<std::string, double>& searchCriteria,
                      const double x, const double y, const double z, const double r, const std::string& world, bool if_max = false) const {
            auto& pool = ConnectionPool::getInstance(config);
            const auto conn = pool.getConnection();
            MYSQL* db = conn->get();

            const long long currentTime = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            const long long timeThreshold = currentTime - static_cast<long long>(searchCriteria.second * 3600);

            const std::string searchPattern = "'%" + escapeString(db, searchCriteria.first) + "%'";
            std::ostringstream sql;
            sql << "SELECT * FROM LOGDATA WHERE (name LIKE " << searchPattern
                << " OR type LIKE " << searchPattern
                << " OR data LIKE " << searchPattern
                << ") AND time >= " << timeThreshold
                << " AND world = '" << escapeString(db, world) << "'"
                << " AND ((pos_x - " << x << ")*(pos_x - " << x
                << ") + (pos_y - " << y << ")*(pos_y - " << y
                << ") + (pos_z - " << z << ")*(pos_z - " << z
                << ")) <= " << (r * r);

            if (!if_max) {
                sql << " LIMIT 10000";
            }
            sql << ";";

            pool.returnConnection(conn);
            return querySQL(sql.str(), result);
        }

        //数据库工具

        static std::vector<std::string> splitString(const std::string& input) {
            std::vector<std::string> result;
            std::string current;
            bool inBraces = false;
            bool inBrackets = false;

            for (const char ch : input) {
                if (ch == '{') {
                    inBraces = true;
                    current += ch;
                } else if (ch == '}') {
                    inBraces = false;
                    current += ch;
                } else if (ch == '[') {
                    inBrackets = true;
                    current += ch;
                } else if (ch == ']') {
                    inBrackets = false;
                    current += ch;
                } else if (ch == ',' && !inBraces && !inBrackets) {
                    if (!current.empty()) {
                        result.push_back(current);
                        current.clear();
                    }
                } else {
                    current += ch;
                }
            }

            if (!current.empty()) {
                result.push_back(current);
            }

            return result;
        }

        static std::vector<int> splitStringInt(const std::string& input) {
            std::vector<int> result;
            std::stringstream ss(input);
            std::string item;

            while (std::getline(ss, item, ',')) {
                if (!item.empty()) {
                    std::stringstream sss(item);
                    int groupid = 0;
                    if (!(sss >> groupid)) {
                        return {};
                    }
                    result.push_back(groupid);
                }
            }

            if (result.empty()) {
                return {};
            }

            return result;
        }

        static std::string vectorToString(const std::vector<std::string>& vec) {
            if (vec.empty()) {
                return "";
            }

            std::ostringstream oss;
            for (size_t i = 0; i < vec.size(); ++i) {
                oss << vec[i];
                if (i != vec.size() - 1 && !(vec[i].empty())) {
                    oss << ",";
                }
            }

            return oss.str();
        }

        static std::string IntVectorToString(const std::vector<int>& vec) {
            if (vec.empty()) {
                return "";
            }

            std::ostringstream oss;
            for (size_t i = 0; i < vec.size(); ++i) {
                oss << vec[i];
                if (i != vec.size() - 1) {
                    oss << ",";
                }
            }

            return oss.str();
        }

        static int stringToInt(const std::string& str) {
            try {
                return std::stoi(str);
            } catch (const std::invalid_argument&) {
                return 0;
            } catch (const std::out_of_range&) {
                return 0;
            }
        }

        static std::string generate_uuid_v4() {
            static thread_local std::mt19937 gen{std::random_device{}()};

            std::uniform_int_distribution<int> dis(0, 15);

            std::stringstream ss;
            ss << std::hex;

            for (int i = 0; i < 8; ++i) ss << dis(gen);
            ss << "-";
            for (int i = 0; i < 4; ++i) ss << dis(gen);
            ss << "-";

            ss << "4";
            for (int i = 0; i < 3; ++i) ss << dis(gen);
            ss << "-";

            ss << (dis(gen) & 0x3 | 0x8);
            for (int i = 0; i < 3; ++i) ss << dis(gen);
            ss << "-";

            for (int i = 0; i < 12; ++i) ss << dis(gen);

            return ss.str();
        }

    private:
        MysqlConfig config;
    };
}
#endif // TIANYAN_DATABASE_H
