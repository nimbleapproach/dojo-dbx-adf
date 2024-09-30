#run against adf tamplate
$templatejson = '../../factory/example/ARMTemplateParametersForFactory.json'
$loopparams = 'devtest','test','preprod','uat' , 'prod'
foreach ($env in $loopparams)
{
    $inputjson = '../../factory/arm-template-parameters-{env}.json'
    $outputjson = '../../factory/arm-parameters-{env}.json'
    ./ps_override_parameters.ps1 -env $env -templatejson $templatejson -inputjson $inputjson -outputjson $outputjson
}

#run against global tamplate parameters
$templatejson = '../../factory/example/adf-ig-dev-westeurope_GlobalParameters.json'
$loopparams = 'uat' #'devtest','test','preprod','uat' , 'prod'
foreach ($env in $loopparams)
{
    $inputjson = '../../factory/arm-template-GlobalParameters-{env}.json'
    $outputjson = '../../factory/arm-GlobalParameters-{env}.json'
    #./ps_override_parameters.ps1 -env $env -templatejson $templatejson -inputjson $inputjson -outputjson $outputjson
}


