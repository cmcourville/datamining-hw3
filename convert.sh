if test -d output
then
    rm -r output	
fi
mkdir output
cp test*.py output/
cp matrix.csv output/
cp cards.txt output/

FILES=problem*.py
for f in $FILES
do
    python convert.py "$f" "output/$f"
done

