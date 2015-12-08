/*
 * \file MathUtils.h
 *
 *
 * \verbatim
        Revision:       $Revision: 1.0 $
        Revision date:  $Date: 2011/12/28 12:10:48 $
        Committed by:   $Author: Hesse $

        This file is part of the PerSyst Monitoring framework

		Copyright (c) 2010-2016, Leibniz-Rechenzentrum (LRZ), Germany
        See the COPYING file in the base directory of the package for details.
   \endverbatim
 */


#ifndef MATHUTILS_H_
#define MATHUTILS_H_
#include <vector>
#include <cmath>
#include <string>
#include <iostream>

typedef float mathType;
using namespace std;

#define def_max(a, b) ( (a)>(b) ? (a) : (b) )
#define def_min(a, b) ( (a)<(b) ? (a) : (b) )

mathType mean(vector<mathType> &num);
//mathType variance(vector<mathType> &num);
//mathType deviation(vector<mathType> &num);

//vector<mathType> calcAlphas(int quNumberIn);
//vector<mathType> adapQuantils(vector<mathType> &quInOut_t0,mathType val_t1, float ac);
/*vector<mathType> adapQuantilsWithQuantiles(
		vector<mathType> &quInOut_t0,
		vector<mathType> const &quIn_t1, float ac);*/
//void adapMean(mathType &val_t0, mathType val_t1, float ac = 0);
//void adapMean4Vector(vector<mathType> &val_t0, vector<mathType> const &val_t1, float ac = 0);
//void meanOfMeans(mathType &val_t0, int &n_all, mathType val_t1, int n_new);
//void meanOfMeans4Vector(vector<mathType> &val_t0, int &n_all, vector<mathType> const &val_t1, int n_new);

//unsigned long getHashKey(string key);
//long getHashKeyLong(string key);
//int getHashKeyInt(string key);
//unsigned int getHashKeyUInt(string key);

/** Divide in two groups
 * Input data vector contains all the numbers to be splitted
 * smallerGroup is the resulting group with the larger elements of the vector
 * biggerGroup is the resulting group with the smaller elements of the vector
 */
//void divideInTwoGroups(vector<mathType>& inputData, vector<mathType>& smallerGroup, vector<mathType>& biggerGroup);

#endif /* MATHUTILS_H_ */
