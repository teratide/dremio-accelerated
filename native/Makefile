# Set the location of the java path and java home
JPATH = /usr/lib64
JAVA_HOME = /usr/lib/jvm/java-1.8.0-openjdk-amd64

all: filter aws

filter: converters javaResizableBuffer assertions
	g++ -fPIC -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -shared -o $(JPATH)/libnative_filter.so src/NativeFilter.cpp -larrow -lre2 Converters.o JavaResizableBuffer.o Assertions.o

aws: converters javaResizableBuffer assertions tidre
	g++ -fPIC -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -shared -o $(JPATH)/libtidre_filter.so src/TidreFilter.cpp -larrow -lfletcher -lre2 Converters.o JavaResizableBuffer.o Assertions.o

tidre:
	g++ -fPIC -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -c src/tidre.h

converters:
	g++ -fPIC -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -c src/jni/Converters.cpp

javaResizableBuffer:
	g++ -fPIC -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -c src/jni/JavaResizableBuffer.cpp

assertions:
	g++ -fPIC -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -c src/jni/Assertions.cpp

clean:
	rm *.o
