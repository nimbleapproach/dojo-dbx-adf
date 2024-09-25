param (
    [parameter(Mandatory = $true)] [String] $env,
    [parameter(Mandatory = $true)] [String] $templatejson,
    [parameter(Mandatory = $true)] [String] $inputjson,
    [parameter(Mandatory = $true)] [String] $outputjson
)

function Process-Json {
    param (
        [Parameter(Mandatory = $true)] [String] $env,
        [Parameter(Mandatory = $true)] [PSCustomObject] $overrideobject,
        [Parameter(Mandatory = $true)] [PSCustomObject] $newobject
    )

    if ($overrideobject.PSObject.Properties.Name -contains 'parameters') {
        $parameters= $overrideobject.parameters
        $newobject=$newobject.parameters
    } else {
        $parameters =$overrideobject
        $newobject=$newobject
    }
        write-host('inputjson: {0}' -f $inputjson)

    if ($newobject) {
    
        $parameters | Get-Member -MemberType NoteProperty | ForEach-Object {
            $key = $_.Name
            
            write-host('key {0}' -f $key)
            if ($newobject | Get-Member -MemberType NoteProperty | Where-Object name -eq $key) {
                Write-Host "A $key value matches template file. continue to override" -ForegroundColor White -BackgroundColor Green

                $getType = $parameters.$key.value.GetType().Name
                Write-Host "$key value is a type: $getType" -ForegroundColor Red -BackgroundColor White

                switch ($getType) {
                    "String" {
                        $value = $parameters."$key".value -replace '{env}', $env -replace '{subscriptionid}', '${{parameters.subscriptionIdDev}}'
                        $newobject.$key.value = $value
                    }
                    "Int32" {
                        $value = [Int32]$parameters."$key".value
                    $newobject.$key.value = $value
                    }
                    "Int64" {
                        $value = [Int64]$parameters."$key".value
                    $newobject.$key.value = $value
                    }
                    "PSCustomObject" {
                        $parameters."$key".value | Get-Member -MemberType NoteProperty | Select-Object -First 1 | ForEach-Object { $key2 = $_.Name }
                        $key2 = $key2 -replace '{env}', $env -replace '{subscriptionid}', '${{parameters.subscriptionIdDev}}'
                        $obj = [PSCustomObject]@{$key2 = @{}}
                        $newobject.$key.value = $obj
                    }
                    default {
                        $value = $parameters."$key".value -replace '{env}', $env -replace '{subscriptionid}', '${{parameters.subscriptionIdDev}}'
                        $newobject.$key.value = $value
                    }
                }
            } else {
                Write-Host "This $key value does NOT exist in the template file! skipping this Key" -ForegroundColor Red -BackgroundColor Black
            }
        }
    } else {
        Write-Host "The 'parameters' property is not found in the new object."
    }
}

function Main {
    param (
        [Parameter(Mandatory = $true)] [String] $env,
        [Parameter(Mandatory = $true)] [String] $templatejson,
        [Parameter(Mandatory = $true)] [String] $inputjson,
        [Parameter(Mandatory = $true)] [String] $outputjson
    )

    $newinputjson = $inputjson -replace '{env}', $env
    $newoutputjson = $outputjson -replace '{env}', $env

    Write-Host ('processing {0}' -f $newinputjson)
    if (Test-Path -Path $newinputjson -PathType Leaf) {
        $overrideobject = Get-Content -Path $newinputjson | ConvertFrom-Json
        $newobject = Get-Content -Path $templatejson | ConvertFrom-Json

        if (Test-Path -Path $newoutputjson -PathType Leaf) {
            Remove-Item $newoutputjson
        }

        Process-Json -env $env -overrideobject $overrideobject -newobject $newobject

        New-Item $newoutputjson
        $newobject | ConvertTo-Json -Depth 10 | Set-Content $newoutputjson
    } else {
        Write-Host ('{0} not found!' -f $newinputjson)
    }
}

# Call the main function
Main -env $env -templatejson $templatejson -inputjson $inputjson -outputjson $outputjson
