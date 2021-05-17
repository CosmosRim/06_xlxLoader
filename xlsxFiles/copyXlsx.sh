#! /bin/sh

declare ori_seq=31

echo "[`date '+%y/%m/%d %H:%M:%S'`]start"

for ((seq=ori_seq; seq <= $1; seq++))
do
    newFile="map${seq}.xlsx"
    `cp map1.xlsx ${newFile}`
done

sed -i "" "s/declare ori_seq=${ori_seq}/declare ori_seq=${seq}/g" copyXlsx.sh

echo "[`date '+%y/%m/%d %H:%M:%S'`]finished"
