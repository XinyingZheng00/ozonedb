#include "lazylog_cli.h"
#include "utils/properties.h"

//sudo GLOG_minloglevel=1 ./basic_client -P /sharedfs/LazyLog-Artifact/cfg/be.prop -P /sharedfs/LazyLog-Artifact/cfg/dl_client.prop -P /sharedfs/LazyLog-Artifact/cfg/rdma.prop -p mode=w

int main(int argc, const char *argv[]) {
    using namespace lazylog;

    Properties prop;
    ParseCommandLine(argc, argv, prop);

    LazyLogClient cli;
    
    cli.Initialize(prop);

    if (prop.GetProperty("mode", "w") == "w") {
        for (int i = 0; i < 10; i++){
            cli.AppendEntryAll("this is a log entry1");
            std::cout << "Wrote entry " << i << std::endl;
        }
    } else {
        int yea = 0, nay = 0;
        std::string data;
        for (int i = 0; i < 11; i++) {
            if (cli.ReadEntry(i, data) > 0)
                yea++;
            else
                nay++;
            std::cout << "Read entry " << i << ": " << data << std::endl;
        }
        std::cout << "Yea: " << yea << ", Nay: " << nay << std::endl;
    }
    
    return 0;
}