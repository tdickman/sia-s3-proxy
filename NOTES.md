curl -A "Sia-Agent" -u "":a46dcccee2a6157ed17cd149bece4076 --data "action=create" "localhost:9980/renter/dir/test-dir"
curl -A "Sia-Agent" "localhost:9980/renter/dir/" | jq
