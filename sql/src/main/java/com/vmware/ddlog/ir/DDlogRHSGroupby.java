/*
 * Copyright (c) 2021 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package com.vmware.ddlog.ir;

import com.facebook.presto.sql.tree.Node;

import javax.annotation.Nullable;

public class DDlogRHSGroupby extends DDlogRuleRHS {
    final String var;
    final DDlogExpression rhsProject;
    final String[] rhsGroupByVars;

    public DDlogRHSGroupby(@Nullable Node node, String var, DDlogExpression rhsProject, String... rhsGroupByVars) {
        super(node);
        this.var = var;
        this.rhsProject = rhsProject;
        this.rhsGroupByVars = rhsGroupByVars;
    }

    @Override
    public String toString() {
        return "var " + this.var + " = " + this.rhsProject.toString() +
                ".group_by((" + String.join(", ", this.rhsGroupByVars) + "))";
    }

    @Override
    public boolean compare(DDlogRuleRHS val, IComparePolicy policy) {
        if (!val.is(DDlogRHSGroupby.class))
            return false;
        DDlogRHSGroupby other = val.to(DDlogRHSGroupby.class);
        if (!policy.compareLocal(this.var, other.var))
            return false;
        if (!this.rhsProject.compare(other.rhsProject, policy))
            return false;
        if (this.rhsGroupByVars.length != other.rhsGroupByVars.length)
            return false;
        for (int i = 0; i < this.rhsGroupByVars.length; i++)
            if (!policy.compareIdentifier(this.rhsGroupByVars[i], other.rhsGroupByVars[i]))
                return false;
        return true;
    }
}
