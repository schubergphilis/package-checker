# NPM Package checker

Converted from the original python script

## how to run


`package_checker  --package-file packages.txt --no-npm --start-path ~/.vscode`

or any other directory you want to scan

## output

The run will output any files that match the version in the package.txt

and create an csv output. Still need some tweaking

```csv
package,version,location,match_package,match_version,dependency,depended_by
@actions/core,1.11.1,/Users/xxxx/.vscode/extensions/ms-toolsai.jupyter-2025.7.0-darwin-arm64,false,false,dev,jupyter@2025.7.0
@actions/core,1.11.1,/Users/xxxx/.vscode/extensions/ms-toolsai.jupyter-2025.8.0-darwin-arm64,false,false,dev,jupyter@2025.8.0
```


## Authors

Jacob Verhoeks <jjverhoeks@schubergphilis.com>