//
// Created by Fabian Nonnenmacher on 15.05.20.
//

#include "Assertions.h"

void exitWithError(const std::string& msg){
    std::cout << "ERROR: " << msg << std::endl;
    exit(-1);
}