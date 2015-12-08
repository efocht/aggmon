/*
 * \file Quantiles.h
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


#ifndef QUANTILES_H_
#define QUANTILES_H_
#include <iostream>
#include <vector>
#include "MathUtils.h"
#include "Errmsg.h"

using namespace std;

static const int NUMBER_QUANTILES = 10;

/*!\class Quantiles
 *
 * \brief Class to calculate percentiles
 *
 * \details Calculates a vector of percentiles and mean value.
 * The percentile is: The kth percentile P_i is that value of x, say x_i,
 * which corresponds to a cumulative frequency of N * i /100, where N is the sample size.
 */
class Quantiles {
private:
	vector<mathType> sortVec; //!< sort vector of observations or samples
	mathType my_min; //!< minimum value
	mathType my_max; //!< maximum value
	mathType my_mean; //!< mean value
	vector<mathType> quantils; //!< percentiles
	unsigned int quantilNumber; //!< number of percentiles

	/** Update minimum maximum and mean value.
	 *
	 */
	void updateMinMaxMean();

	/** Calculate percentiles
	 * The ith percentile P_i is that value of x, say x_i,
	 * which corresponds to a cumulative frequency of N * i/100, where N is the sample size.
	 */
	void updateQuantils();

public:

	/** Constructor
	 *
	 */
	Quantiles();

	/** Constructor
	 * @param	valVec	[in]	vector with observations
	 * @param	quNmb	[in]	number of percentiles to be calculated (default is 10)
	 */
	Quantiles(const vector<mathType> &valVec, unsigned int quNmb = NUMBER_QUANTILES);

	/** Destructor
	 *
	 */
	virtual ~Quantiles();

	/** Sets and sorts the vector
	 * Sets and sorts the vector and calls the updateQuantils method
	 * @param	inVec	[in]	samples or observations
	 */
	void setSortVec(vector<mathType> &inVec);

	/** simple getter
	 * @return	sorted vector of observation
	 */
	vector<mathType> getSortVec() const {
		return sortVec;
	}

	/** simple getter
	 * @return	number of elements in the sorted vector
	 */
	int getNumberOfElements(){
		return sortVec.size();
	}

	/** simple getter
	 * @return	maximum
	 */
	mathType getMax() const {
		return my_max;
	}

	/** simple getter
	 * @return	minimum
	 */
	mathType getMin() const {
		return my_min;
	}

	/** simple getter
	 * @return	mean value
	 */
	mathType getMean() const {
		return my_mean;
	}

	/** simple setter
	 * @param	k	[in]	number of percentiles
	 */
	void setQuantilNumber(int k) {
		this->quantilNumber = k;
	}

	/** simple getter
	 * @return	number of percentiles
	 */
	int getQuantilNumber() const {
		return quantilNumber;
	}

	/** simple getter
	 * @return percentiles
	 */
	vector<mathType> getQuantils() const {
		return quantils;
	}

	/** Insert element in sort list
	 * @param	val	[in]	value to be inserted
	 */
	/*void insertElInSortList(mathType val);*/

	/** Insert vector in sort list
	 * @param	val	[in]	vector of values to be inserted
	 */
	/*void insertVecInSortList(vector<mathType> &val);*/
};

#endif /* QUANTILES_H_ */
