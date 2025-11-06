//
// Created by yuhang on 2025/11/6.
//

#ifndef TIANYAN_TIANYANPROTECT_H
#define TIANYAN_TIANYANPROTECT_H
#include <Global.h>

class TianyanProtect {
public:
    explicit TianyanProtect(endstone::Plugin &plugin) : plugin_(plugin){}

private:
    endstone::Plugin &plugin_;
};


#endif //TIANYAN_TIANYANPROTECT_H