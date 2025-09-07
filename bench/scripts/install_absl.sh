mkdir temp_objs
idx=0
for lib in libabsl_*.a; do
  mkdir temp_objs/dir_$idx
  cd temp_objs/dir_$idx
  ar x ../../$lib
  for obj in *.o; do
    mv "$obj" "${idx}_${obj}"  # Rename to avoid collision
  done
  cd ../..
  idx=$((idx + 1))
done
cd temp_objs
ar cr ../libabsl.a */*.o
cd ..
ranlib libabsl.a
rm -rf temp_objs
