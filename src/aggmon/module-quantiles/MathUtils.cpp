/*
 * \file MathUtils.cpp
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
#include "MathUtils.h"
#include <algorithm>

mathType mean(vector<mathType> &num) {
	int total_numbers = num.size();
	mathType sum = 0, my_mean = 0;
	for (int j = 0; j < total_numbers; j++) {
		sum += num[j];
	}
	my_mean = (sum / total_numbers);
	return my_mean;
}

/*mathType variance(vector<mathType> &num) {
	int total_numbers = num.size();
	mathType my_mean = 0;
	mathType var = 0.0;
	if (total_numbers > 1) {
		my_mean = mean(num);
		for (int i = 0; i < total_numbers; ++i) {
			var += ((num[i] - my_mean) * (num[i] - my_mean));
		}
		var /= (total_numbers - 1);
	}
	return var;
}*/

/*mathType deviation(vector<mathType> &num) {
	int total_numbers = num.size();
	mathType var = 0.0, dev = 0.0;
	if (total_numbers > 1) {
		var = variance(num);
		dev = sqrt(var);
	}
	return dev;
}*/

/*vector<mathType> calcAlphas(int quNumberIn) {
	vector<mathType> alpha_v(quNumberIn + 1); //+Min;
	for (unsigned int idx = 0; idx < alpha_v.size(); idx++) {
		alpha_v[idx] = (1.0 * idx) / (quNumberIn);
	}
	return alpha_v;
}*/

/*vector<mathType> adapQuantils(vector<mathType> &quInOut_t0,
		mathType val_t1, float ac) {
	int idx = 0;
	vector<mathType> alpha_v;
	alpha_v.resize(quInOut_t0.size());
	for (vector<mathType>::iterator v_it = quInOut_t0.begin(); v_it != quInOut_t0.end(); ++v_it) {
		alpha_v[idx] = (1.0 * idx) / (quInOut_t0.size() - 1); //da Min in vector
		if (val_t1 < quInOut_t0[idx]) {
			quInOut_t0[idx] = quInOut_t0[idx] - (ac * (1.0 - alpha_v[idx]));
		} else {
			quInOut_t0[idx] = quInOut_t0[idx] + (ac * alpha_v[idx]);
		}
		idx++;
	}
	return alpha_v;
}*/

/*
 * Function dos'nt working satisfactory.... using ac=0.5 -> it's a kind of  mean calculation...
 */
/*vector<mathType> adapQuantilsWithQuantiles(
		vector<mathType> &quInOut_t0,
		vector<mathType> const &quIn_t1, float ac) {
	vector<mathType> alpha_v;
	if (quInOut_t0.size() == quIn_t1.size()) {
		alpha_v.resize(quInOut_t0.size());
		for (unsigned int idx = 0; idx < quInOut_t0.size(); idx++) {
			quInOut_t0[idx] = quInOut_t0[idx] + ac * (quIn_t1[idx]
					- quInOut_t0[idx]);
			idx++;
		}
	}
	return alpha_v;
}*/

/*void adapMean(mathType &val_t0, mathType val_t1, float ac) {
	val_t0 = val_t0 + (ac) * (val_t1 - val_t0);
}*/

/*void adapMean4Vector(vector<mathType> &val_t0,
		vector<mathType> const &val_t1, float ac) {
	if (val_t0.size() == val_t1.size()) {
		for (unsigned int ii = 0; ii < val_t0.size(); ii++) {
			adapMean(val_t0[ii], val_t1[ii], ac);
		}
	} else {
		cout << "MathUtils::adapMean4Vector:  vector  size not equal (old: "
				<< val_t0.size() << "; new: " << val_t1.size() << ")\n";
	}
}*/

/*void meanOfMeans(mathType &val_t0, int &n_all, mathType val_t1, int n_new) {
	int newAll=n_new+n_all;
	val_t0 = ((val_t0*n_all) + (val_t1 * n_new))/mathType(newAll);
	n_all=newAll;
}*/

/*void meanOfMeans4Vector(vector<mathType> &val_t0, int &n_all,
		vector<mathType> const &val_t1, int n_new) {
	int newAll=n_new+n_all;
	if (val_t0.size() == val_t1.size()) {
		for (unsigned int ii = 0; ii < val_t0.size(); ii++) {
			meanOfMeans(val_t0[ii],n_all, val_t1[ii], n_new);
		}
	} else {
		cout << "MathUtils::adapMean4Vector:  vector  size not equal (old: "
						<< val_t0.size() << "; new: " << val_t1.size() << ")\n";
	}
}*/

/*
unsigned long getHashKey(string key) {
	unsigned long res = 0;
	//	key="51;367839;2009-11-11 13:57:55;111;0.000000;";
	std::string::const_iterator p = key.begin();
	std::string::const_iterator end = key.end();
	while (p != end) {
		res = (res << 1) ^ *p;
		p++;
	}
	return res;
}

long getHashKeyLong(string key) {
	long res = 0;
	std::string::const_iterator p = key.begin();
	std::string::const_iterator end = key.end();
	while (p != end) {
		res = (res << 1) ^ *p;
		p++;
	}
	return res;
}
int getHashKeyInt(string key) {
	int res = 0;
	std::string::const_iterator p = key.begin();
	std::string::const_iterator end = key.end();
	while (p != end) {
		res = (res << 1) ^ *p;
		p++;
	}
	return res;
}

unsigned int getHashKeyUInt(string key) {
	unsigned int res = 0;
	std::string::const_iterator p = key.begin();
	std::string::const_iterator end = key.end();
	while (p != end) {
		res = (res << 1) ^ *p;
		p++;
	}
	return res;
}

void divideInTwoGroups(vector<mathType>& inputData, vector<
		mathType>& smallerGroup, vector<mathType>& biggerGroup) {
	sort(inputData.begin(), inputData.end());
	//find max distance between elements
	float maxDistance = 0;
	int posMax = -1;
	for (unsigned int i = 0; i < inputData.size() - 1; ++i) {
		if (maxDistance < (inputData[i + 1] - inputData[i])) {
			maxDistance = inputData[i + 1] - inputData[i];
			posMax = i + 1;
		}
	}
	if (posMax != -1) {
		for (unsigned int i = 0; i < posMax; ++i) {
			smallerGroup.push_back(inputData[i]);
		}
		for (unsigned int i = posMax; i < inputData.size(); ++i) {
			biggerGroup.push_back(inputData[i]);
		}
	}
}
*/
