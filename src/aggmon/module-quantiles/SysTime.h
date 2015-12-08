/**
 * \file SysTime.h
 *
 * \verbatim
        Revision:       $Revision: 1.0 $
        Revision date:  $Date: 2011/12/28 12:10:48 $
        Committed by:   $Author: Hesse, Guillen $

        This file is part of the PerSyst Monitoring framework

		Copyright (c) 2010-2016, Leibniz-Rechenzentrum (LRZ), Germany
        See the COPYING file in the base directory of the package for details.
   \endverbatim
 */


#ifndef SYSTIME_H_
#define SYSTIME_H_

#include <sys/time.h>
#include <string.h>
#include "StringUtils.h"

/** get system time in seconds
 * @return system time in seconds
 */
int getTimeInSeconds();

/**get system time in milliseconds
 * @return system time in milliseconds
 */
long getTimeInMilliSeconds();

/** get the microseconds part of current time
 * @return microseconds
 */
long getOnlyMicroSeconds();

/** get time in formatted string
 * @return time as string
 */
//std::string getTimeInSecondsString();

/** calculate rest time given the end time
 * Calculate end time minus current time.
 * @param endTime	[in]	end time
 * @return rest time until endtime
 */
int calcRestTime(int endTime);

/** get date as string
 * @return date as string
 */
//std::string getDateAsString(int timestamp);

#endif /* SYSTIME_H_ */
