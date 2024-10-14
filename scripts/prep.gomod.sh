set -e  # exit on error

echo 'prep.gomod.sh'
go mod edit -dropreplace='github.com/flarco/g' go.mod
go mod edit -dropreplace='github.com/slingdata-io/sling-cli' go.mod
go mod edit -droprequire='github.com/slingdata-io/sling' go.mod
# go get github.com/flarco/g@HEAD
# go get github.com/slingdata-io/sling-cli@HEAD
go mod tidy