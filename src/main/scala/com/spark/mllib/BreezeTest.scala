package com.spark.mllib

import breeze.linalg.{Axis, DenseMatrix, DenseVector, det, diag, eig, inv, max, min, pinv, rank, sum, svd}
import breeze.numerics.{abs, ceil, floor, round, signum}
import breeze.stats.distributions.Rand

object BreezeTest {

  def main(args: Array[String]): Unit = {
    zeros
  }

  def zeros: Unit = {
    // 矩阵
    val data = DenseMatrix.zeros[Int](3, 4)
    printlnData(data)
    // 全0向量
    val vector = DenseVector.zeros[Int](2)
    printlnData(vector)
    // 按数值填充向量
    val fourVec = DenseVector.fill(3, 4)
    printlnData(fourVec)
    //随机向量
    val rangeVector = DenseVector.range(1, 9, 2)
    printlnData(rangeVector)
    //单位矩阵
    val eyeVector = DenseMatrix.eye[Int](4)
    printlnData(eyeVector)
    // 对角矩阵
    val diagVector = diag(DenseVector(3, 4, 5))
    printlnData(diagVector)
    //按照行创建矩阵
    val lineMatrix = DenseMatrix((4.0, 5.0, 6.0), (7.0, 8.0, 9.0))
    printlnData(lineMatrix)
    //按照行创建向量
    val lineVector = DenseVector((4.0, 5.0, 6.0, 7.0, 8.0, 9.0))
    printlnData(lineVector)
    //向量转置
    val tVector = DenseVector((4.0, 5.0, 6.0, 7.0, 8.0, 9.0)).t
    printlnData(tVector)
    //从函数创建向量
    // DenseVector(0, 1, 4, 9, 16) 不包含5
    val funcVector = DenseVector.tabulate(5)(i => i * i)
    printlnData(funcVector)
    // 包含DenseVector(0, 1, 4, 9, 16, 25)
    val funcVector2 = DenseVector.tabulate(0 to 5)(i => i * i)
    printlnData(funcVector2)
    //从函数创建矩阵
    val tabMa = DenseMatrix.tabulate(3, 4) { case (i, j) => i * i + j * j }
    printlnData(tabMa)
    //从数组创建向量
    val arrVector = new DenseVector[Double](Array(2.0, 5.0, 8.0))
    printlnData(arrVector)
    val arrMairx = new DenseMatrix[Double](3, 2, Array(1.0, 4.0, 7.0, 3.0, 6.0, 9.0))
    printlnData(arrMairx)
    //0 到 1的随机向量
    printlnData(DenseVector.rand(3, Rand.uniform))
    printlnData(DenseVector.rand(3, Rand.gaussian))
    // 0-1 随机举证
    printlnData(DenseMatrix.rand(3, 2, Rand.uniform))
    printlnData(DenseMatrix.rand(3, 2, Rand.gaussian))

    val value = new DenseVector[Int](Array(10 to 20: _*))
    printlnData(value)
    printlnData(value(0))
    printlnData(value(1 to 4))
    // 指定步长
    printlnData(value(1 to 8 by 1))
    // 从头到尾
    printlnData(value(1 to -1))

    val myMa = DenseMatrix.tabulate(4, 5) { case (i, j) => Rand.randInt(10).get() }

    /*   0   1 2   3   4
     |0| 9  1  3   8   7
     |1| 7  2  5   7   6
     |2| 8  5  7   12  11
     |3| 0  6  15  11  15

     */
    printlnData(myMa)
    // 指定位置
    printlnData(myMa(0, 3))
    // 指定列
    printlnData(myMa(::, 3))
    // 指定行
    printlnData(myMa(1, ::))
    // 调整矩阵
    /**
      * 0  1  2  3 4
      * 0 9  2  7   11
      * 1 7  5  15  7
      * 2 8  6  8   6
      * 3 0  3  7   11
      * 4 1  5  12  15
      */
    printlnData(myMa.reshape(5, 4))
    // 矩阵转向量
    printlnData(myMa.toDenseVector)
    //复制下三角 //复制上三角
    /*lowerTriangular(myMa)
    upperTriangular(myMa)*/
    // 矩阵复制
    printlnData(myMa.copy)
    // 取对角 必须是正方形
    //    printlnData(diag(myMa))
    //子集赋数值

    /**
      * TODO 当这里面的值为float时运行会出现如下警告
      * 五月 17, 2019 10:57:06 上午 com.github.fommil.netlib.BLAS <clinit>
      * 警告: Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
      * 五月 17, 2019 10:57:06 上午 com.github.fommil.netlib.BLAS <clinit>
      * 警告: Failed to load implementation from: com.github.fommil.netlib.NativeRefBLAS
      *
      */
    val a = DenseMatrix((1, 2), (3, 4))
    val b = DenseMatrix((5, 6), (7, 8))
    printlnData(a)
    printlnData(b)
    //    myMa(1 to 4):=5
    // 垂直连接矩阵 vertcat， 横向连接 horzcat
    printlnData(DenseMatrix.vertcat(a, b))
    printlnData(DenseMatrix.horzcat(a, b))
    // 加减乘除
    printlnData(a + b)
    printlnData(a * b)
    printlnData(a / b)
    /* Error:(128, 19) value < is not a member of breeze.linalg.DenseMatrix[Int] printlnData(a < b)
      printlnData(a < b)
        printlnData(a > b)*/
    printlnData(a == b)
    printlnData(a += 2)
    //向量点积
    val dotData = DenseVector(1, 2, 3, 4) dot DenseVector(2, 2, 1, 1)
    printlnData(dotData)
    //元素最大值
    printlnData(max(a))
    //元素最小值
    printlnData(min(a))

    printlnData(sum(a))

    //每一列求和
    printlnData(sum(a, Axis._0))
    // 每一行求和
    printlnData(sum(a, Axis._1))

    //    Breeze线性代数函数
    val f = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0), (7.0, 8.0, 9.0))
    val g = DenseMatrix((1.0, 1.0, 1.0), (1.0, 1.0, 1.0), (1.0, 1.0, 1.0))
    //线性求解，AX = B，求解X
    printlnData(f \ g)
    //转置
    printlnData(f.t)
    //求特征值
    printlnData(det(f))
    //求逆
    printlnData(inv(f))
    //求伪逆
    printlnData(pinv(f))
    //特征值和特征向量
    printlnData(eig(f))
    //奇异值分解
    val svd.SVD(u, s, v) = svd(g)
    println(u)
    //求矩阵的秩
    printlnData(rank(f))
    //矩阵长度
    printlnData(f.size)
    //矩阵行数
    printlnData(f.rows)
    //矩阵列数
    printlnData(f.cols)
    ///Breeze取整函数
    val h = DenseVector(-1.2, 0.7, 2.3)
    printlnData( //四舍五入
      round(h))
    //大于它的最小整数
    printlnData(ceil(h))
    printlnData( //小于它的最大整数
      floor(h))
    printlnData( //符号函数
      signum(h))
    printlnData( //取绝对值
      abs(h)
    )
  }


  def printlnData(data: Any): Unit = {
    println(data)
    println("---------------------------------------------\n")
  }
}
