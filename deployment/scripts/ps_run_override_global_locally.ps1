
$newinputjson = '../../factory/arm-template-GlobalParameters-uat.json'
  
#$newinputjson = '../../factory/arm-template-parameters-uat.json'  
$overrideobject = Get-Content -Path $newinputjson | ConvertFrom-Json

 
    $parameters = if ($overrideobject.PSObject.Properties.Name -contains 'parameters') {
        $overrideobject.parameters
    } else {
        $overrideobject
    }
     $parameters | Get-Member -MemberType NoteProperty | ForEach-Object {
             $key = $_.Name
            write-host('key {0}' -f $key)              
           } 