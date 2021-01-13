# Set the location of the java path and java home
JPATH = /usr/lib64
JAVA_HOME = /usr/lib/jvm/java-1.8.0-openjdk-amd64

all: filter

filter: converters javaResizableBuffer assertions
	g++ -fPIC -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -shared -o $(JPATH)/libNativeFilter.so src/NativeFilter.cpp -larrow Converters.o JavaResizableBuffer.o Assertions.o

converters:
	g++ -fPIC -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -c src/jni/Converters.cpp

javaResizableBuffer:
	g++ -fPIC -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -c src/jni/JavaResizableBuffer.cpp

assertions:
	g++ -fPIC -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -c src/jni/Assertions.cpp

clean:
	rm *.o
