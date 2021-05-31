# Linux
````shell
sdk use java 21.0.0.r11
gu install native-image
./gradlew :cli:nativeImage
````

# Windows
Drink :coffee:  

## 1. GraalVM
Download GraalVM  
https://github.com/graalvm/graalvm-ce-builds/releases/tag/vm-21.1.0
````shell
setx /M PATH "C:\java\graalvm-ce-java11-21.1.0\bin;%PATH%"
setx /M JAVA_HOME "C:\java\graalvm-ce-java11-21.1.0\"
gu install native-image
````

## 2. VS 2019
Download VS 2019
https://visualstudio.microsoft.com/vs/
Select individual components :  
- C++/CLI support for v142 build tools (latest)  
- MSVC v142 - VS 2019 C++ x64/x86 build tools (latest)  
- Windows Universal CRT SDK
- Windows 10 SDK (10.0.19041.0 or later)  

Source (**thank you**) : https://leward.eu/2020/10/21/native-gui-app-with-javafx-windows.html
  
## 3. Build
Run the new shell : **x64 Native Tools Command Prompt for VS 2019**
````shell
native-image -cp H:\projects\ns4kafka\cli\build\libs\kafkactl-0.1.jar -H:+ReportUnsupportedElementsAtRuntime -H:Name=kafkactl -H:IncludeResourceBundles=org.ocpsoft.prettytime.i18n.Resources --no-server com.michelin.ns4kafka.cli.KafkactlCommand
````
