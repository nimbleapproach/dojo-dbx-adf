param
(
    [parameter(Mandatory = $true)] [String] $AzureDevOpsBranch,
    [parameter(Mandatory = $true)] [String] $Token,
    [parameter(Mandatory = $true)] [String] $DatabricksRepoURI
)
$JsonBody = @{
    branch = $AzureDevOpsBranch
    } | ConvertTo-Json
$SecureToken = $Token | ConvertTo-SecureString -AsPlainText -Force
$Params = @{
    Method = "Patch"
    Uri = $DatabricksRepoURI
    Authentication = "Bearer"
    Token = $SecureToken
    Body = $JsonBody
    ContentType = "application/json"
}
Invoke-RestMethod @Params