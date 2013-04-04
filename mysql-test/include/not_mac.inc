if (`select substring(convert(@@version_compile_os using latin1),1,3) = 'osx'`)
{
     skip Test is known failure on Mac;
}
