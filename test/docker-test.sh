#!ash
trap "echo TRAPed signal" HUP INT QUIT TERM

mysqld --bind-address=127.0.0.1 --plugin-load-add=ha_connect -u root &
sleep 5s
mysqladmin create gapminder --default-character-set=utf8mb4

echo "Ready to start tests!"
npm run test-cli
npm run test-service