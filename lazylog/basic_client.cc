#include "lazylog_cli.h"
#include "utils/properties.h"

int main(int argc, const char *argv[]) {
    using namespace lazylog;

    Properties prop;
    ParseCommandLine(argc, argv, prop);

    LazyLogClient cli;
    
    cli.Initialize(prop);

    if (prop.GetProperty("mode", "w") == "w") {
        for (int i = 0; i < 10; i++){
            cli.AppendEntryQuorum("this is a log entry");
            std::cout << "Wrote entry " << i << std::endl;
        }
    } else {
        int yea = 0, nay = 0;
        std::string data;
        for (int i = 15000; i < 45000; i++) {
            if (cli.SpecReadEntry(i, data) > 0)
                yea++;
            else
                nay++;
            std::cout << "Read entry " << i << ": " << data << std::endl;
        }
        std::cout << "Yea: " << yea << ", Nay: " << nay << std::endl;
    }
    
    return 0;
}