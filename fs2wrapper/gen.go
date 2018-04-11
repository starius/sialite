package fs2wrapper

//go:generate fs2-generic --output=siacoinoutput/wrapper.go --package-name=siacoinoutput bptree --key-type=github.com/NebulousLabs/Sia/crypto/Hash --key-size=32 --key-serializer=github.com/starius/sialite/fs2wrapper/hash/Serialize --key-deserializer=github.com/starius/sialite/fs2wrapper/hash/Deserialize --key-empty=crypto.Hash{} --value-type=Location --value-serializer=Serialize --value-deserializer=Deserialize --value-empty=Location{}
//go:generate go fmt siacoinoutput/wrapper.go
//go:generate fs2-generic --output=transaction/wrapper.go --package-name=transaction bptree --key-type=github.com/NebulousLabs/Sia/crypto/Hash --key-size=32 --key-serializer=github.com/starius/sialite/fs2wrapper/hash/Serialize --key-deserializer=github.com/starius/sialite/fs2wrapper/hash/Deserialize --key-empty=crypto.Hash{} --value-type=Location --value-serializer=Serialize --value-deserializer=Deserialize --value-empty=Location{}
//go:generate go fmt transaction/wrapper.go
