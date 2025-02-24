{-
Copyright (c) 2018-2021 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
-}

{-# LANGUAGE TupleSections, LambdaCase, RecordWildCards #-}

{- |
Module     : DatalogProgram
Description: Helper functions for manipulating 'DatalogProgram'.
-}
module Language.DifferentialDatalog.DatalogProgram (
    progExprMapCtxM,
    progExprMapCtx,
    progTypeMapM,
    progTypeMap,
    progRHSMapM,
    progRHSMap,
    progAtomMapM,
    progAtomMap,
    progAttributeMapM,
    DepGraphNode(..),
    depNodeIsRel,
    depNodeIsApply,
    DepGraph,
    progDependencyGraph,
    depGraphToDot,
    progMirrorInputRelations,
    progOutputInternalRelations,
    progInjectDebuggingHooks,
    ruleExprMapCtxM
)
where

import qualified Data.Graph.Inductive              as G
import qualified Data.Map                          as M
import Data.Maybe
import Data.Char
import Control.Monad.Identity
import qualified Data.Text.Lazy                    as T
import qualified Data.GraphViz                     as GV
import qualified Data.GraphViz.Attributes.Complete as GV
import qualified Data.GraphViz.Printing            as GV

import {-# SOURCE #-} Language.DifferentialDatalog.Debug
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import {-# SOURCE #-} Language.DifferentialDatalog.Module
import {-# SOURCE #-} Language.DifferentialDatalog.Rule
import {-# SOURCE #-} Language.DifferentialDatalog.Type

-- | Map function 'fun' over all expressions in a program
progExprMapCtxM :: (Monad m) => DatalogProgram -> (ECtx -> ENode -> m Expr) -> m DatalogProgram
progExprMapCtxM d fun = do
    rels' <- traverse (relExprMapCtxM fun) $ progRelations d
    idxs' <- traverse (\idx -> do atom' <- atomExprMapCtxM fun (CtxIndex idx) (idxAtom idx)
                                  return idx{idxAtom = atom'}) $ progIndexes d
    funcs' <- traverse (mapM (\f -> do e <- case funcDef f of
                                                 Nothing -> return Nothing
                                                 Just e  -> Just <$> exprFoldCtxM fun (CtxFunc f) e
                                       return f{funcDef = e}))
                       $ progFunctions d
    rules' <- mapM (ruleExprMapCtxM fun) $ progRules d
    return d{ progFunctions = funcs'
            , progRelations = rels'
            , progIndexes   = idxs'
            , progRules     = rules'}

relExprMapCtxM :: (Monad m) => (ECtx -> ENode -> m Expr) -> Relation -> m Relation
relExprMapCtxM fun rel = do
    pkey' <- mapM (\key -> do e' <- exprFoldCtxM fun (CtxKey rel) $ keyExpr key
                              return key{keyExpr = e'})
             $ relPrimaryKey rel
    return rel{relPrimaryKey = pkey'}

ruleExprMapCtxM :: (Monad m) => (ECtx -> ENode -> m Expr) -> Rule -> m Rule
ruleExprMapCtxM fun r = do
    lhs <- mapIdxM (\(RuleLHS p a l) i -> RuleLHS p <$> atomExprMapCtxM fun (CtxRuleLAtom r i) a
                                                    <*> mapM (exprFoldCtxM fun (CtxRuleLLocation r i)) l)
                   $ ruleLHS r
    rhs <- mapIdxM (\x i -> rhsExprMapCtxM fun r i x) $ ruleRHS r
    return r{ruleLHS = lhs, ruleRHS = rhs}

atomExprMapCtxM :: (Monad m) => (ECtx -> ENode -> m Expr) -> ECtx -> Atom -> m Atom
atomExprMapCtxM fun ctx a = do
    v <- exprFoldCtxM fun ctx $ atomVal a
    return a{atomVal = v}

rhsExprMapCtxM :: (Monad m) => (ECtx -> ENode -> m Expr) -> Rule -> Int -> RuleRHS -> m RuleRHS
rhsExprMapCtxM fun r rhsidx l@RHSLiteral{}   = do
    a <- atomExprMapCtxM fun (CtxRuleRAtom r rhsidx) (rhsAtom l)
    return l{rhsAtom = a}
rhsExprMapCtxM fun r rhsidx c@RHSCondition{} = do
    e <- exprFoldCtxM fun (CtxRuleRCond r rhsidx) (rhsExpr c)
    return c{rhsExpr = e}
rhsExprMapCtxM fun r rhsidx a@RHSGroupBy{} = do
    e <- exprFoldCtxM fun (CtxRuleRProject r rhsidx) (rhsProject a)
    g <- exprFoldCtxM fun (CtxRuleRGroupBy r rhsidx) (rhsGroupBy a)
    return a{rhsGroupBy = g, rhsProject = e}
rhsExprMapCtxM fun r rhsidx m@RHSFlatMap{}   = do
    e <- exprFoldCtxM fun (CtxRuleRFlatMap r rhsidx) (rhsMapExpr m)
    vs <- exprFoldCtxM fun (CtxRuleRFlatMapVars r rhsidx) (rhsVars m)
    return m{rhsVars = vs, rhsMapExpr = e}
rhsExprMapCtxM fun r rhsidx i@RHSInspect{}   = do
    e <- exprFoldCtxM fun (CtxRuleRInspect r rhsidx) (rhsInspectExpr i)
    return i{rhsInspectExpr = e}

progExprMapCtx :: DatalogProgram -> (ECtx -> ENode -> Expr) -> DatalogProgram
progExprMapCtx d fun = runIdentity $ progExprMapCtxM d  (\ctx e -> return $ fun ctx e)


-- | Apply function to all types referenced in the program
progTypeMapM :: (Monad m) => DatalogProgram -> (Type -> m Type) -> m DatalogProgram
progTypeMapM d@DatalogProgram{..} fun = do
    ts <- traverse(\(TypeDef p atrs n a t) -> TypeDef p atrs n a <$> mapM (typeMapM fun) t) progTypedefs
    fs <- traverse (mapM (\f -> do ret <- typeMapM fun $ funcType f
                                   as  <- mapM (\a -> setType a <$> (typeMapM fun $ typ a)) $ funcArgs f
                                   def <- mapM (exprTypeMapM fun) $ funcDef f
                                   return f{ funcType = ret, funcArgs = as, funcDef = def }))
                   progFunctions
    trans <- traverse (\t -> do inputs  <- mapM (\i -> do t' <- hotypeTypeMapM (hofType i) fun
                                                          return i{hofType = t'}) $ transInputs t
                                outputs <- mapM (\o -> do t' <- hotypeTypeMapM (hofType o) fun
                                                          return o{hofType = t'}) $ transOutputs t
                                return t{ transInputs = inputs, transOutputs = outputs }) progTransformers
    rels <- traverse (\rel -> setType rel <$> (typeMapM fun $ typ rel)) progRelations
    idxs <- traverse (\idx -> do vars <- mapM (\v -> setType v <$> (typeMapM fun $ typ v)) $ idxVars idx
                                 atomval <- exprTypeMapM fun $ atomVal $ idxAtom idx
                                 let atom = (idxAtom idx) { atomVal = atomval }
                                 return idx { idxVars = vars, idxAtom = atom }) progIndexes
    rules <- mapM (ruleTypeMapM fun) progRules
    return d { progTypedefs     = ts
             , progFunctions    = fs
             , progTransformers = trans
             , progRelations    = rels
             , progIndexes      = idxs
             , progRules        = rules
             }

hotypeTypeMapM :: (Monad m) => HOType -> (Type -> m Type) -> m HOType
hotypeTypeMapM hot@HOTypeRelation{..} fun = do
    t <- typeMapM fun hotType
    return hot { hotType = t }
hotypeTypeMapM hot@HOTypeFunction{..} fun = do
    ret <- typeMapM fun hotType
    as  <- mapM (\f -> setType f <$> (typeMapM fun $ typ f)) hotArgs
    return hot { hotArgs = as, hotType = ret }

progTypeMap :: DatalogProgram -> (Type -> Type) -> DatalogProgram
progTypeMap d fun = runIdentity $ progTypeMapM d (return . fun)

-- | Apply function to all rule RHS terms in the program
progRHSMapM :: (Monad m) => DatalogProgram -> (Rule -> Int -> RuleRHS -> m [RuleRHS]) -> m DatalogProgram
progRHSMapM d fun = do
    rs <- mapM (\r -> do
                 rhs <- concat <$> (mapIdxM (\rhs rhs_idx -> fun r rhs_idx rhs) $ ruleRHS r)
                 return r { ruleRHS = rhs })
               $ progRules d
    return d { progRules = rs }

progRHSMap :: DatalogProgram -> (Rule -> Int -> RuleRHS -> [RuleRHS]) -> DatalogProgram
progRHSMap d fun = runIdentity $ progRHSMapM d (\rl rhs_idx rhs -> return $ fun rl rhs_idx rhs)

-- | Apply function to all atoms in the program
progAtomMapM :: (Monad m) => DatalogProgram -> (Atom -> m Atom) -> m DatalogProgram
progAtomMapM d fun = do
    rs <- mapM (\r -> do
                 lhs <- mapM (\lhs -> do a <- fun $ lhsAtom lhs
                                         return $ lhs{ lhsAtom = a })
                        $ ruleLHS r
                 rhs <- mapM (\case
                               lit@RHSLiteral{} -> do a <- fun $ rhsAtom lit
                                                      return lit { rhsAtom = a }
                               rhs              -> return rhs)
                        $ ruleRHS r
                 return r { ruleLHS = lhs, ruleRHS = rhs })
               $ progRules d
    is <- mapM (\i -> do a <- fun $ idxAtom i
                         return i { idxAtom = a })
               $ progIndexes d
    return d { progRules = rs
             , progIndexes = is }

progAtomMap :: DatalogProgram -> (Atom -> Atom) -> DatalogProgram
progAtomMap d fun = runIdentity $ progAtomMapM d (return . fun)

-- | Apply function to all attributes in the program.
progAttributeMapM :: (Monad m) => DatalogProgram -> (Attribute -> m Attribute) -> m DatalogProgram
progAttributeMapM d fun = do
    tdefs' <- M.traverseWithKey (\_ tdef@TypeDef{..} -> do
        atrs' <- mapM fun tdefAttrs
        t' <- mapM (typeAttributeMapM fun) tdefType
        return $ tdef{ tdefAttrs = atrs'
                     , tdefType = t' }) $ progTypedefs d
    return d{progTypedefs = tdefs'}

typeAttributeMapM :: (Monad m) => (Attribute -> m Attribute) -> Type -> m Type
typeAttributeMapM fun t@TStruct{..} = do
    cs' <- mapM (consAttributeMapM fun) typeCons
    return t{typeCons = cs'}
typeAttributeMapM _ t = return t

consAttributeMapM :: (Monad m) => (Attribute -> m Attribute) -> Constructor -> m Constructor
consAttributeMapM fun c@Constructor{..} = do
    attrs' <- mapM fun consAttrs
    fields' <- mapM (fieldAttributeMapM fun) consArgs
    return c{ consAttrs = attrs'
            , consArgs = fields'}

fieldAttributeMapM :: (Monad m) => (Attribute -> m Attribute) -> Field -> m Field
fieldAttributeMapM fun f@Field{..} = do
    attrs' <- mapM fun fieldAttrs
    return f{fieldAttrs = attrs'}

data DepGraphNode = DepNodeRel   String
                  | DepNodeApply Apply
                  deriving (Eq)

instance Show DepGraphNode where
    show (DepNodeRel rel) = rel
    show (DepNodeApply a) = show a

depNodeIsApply :: DepGraphNode -> Bool
depNodeIsApply (DepNodeApply _) = True
depNodeIsApply _                = False

depNodeIsRel :: DepGraphNode -> Bool
depNodeIsRel (DepNodeRel _) = True
depNodeIsRel _              = False

type DepGraph = G.Gr DepGraphNode Bool

-- | Dependency graph among program relations.  An edge from Rel1 to
-- Rel2 means that there is a rule with Rel1 in the right-hand-side,
-- and Rel2 in the left-hand-side.  Edge label is equal to the
-- polarity with which Rel1 occurs in the rule.
--
-- We do not add a dependency if Rel1 is delayed in the RHS, as such
-- dependencies cannot be part of a cycle.
--
-- In addition, we conservatively add both a positive and a negative edge
-- from Rel1 to Rel2 if they appear respectively as input and output of a
-- transformer application (since we currently don't have a way of knowing
-- if the transformer is monotonic).
--
-- Assumes that rules and relations have been validated before calling
-- this function.
progDependencyGraph :: DatalogProgram -> DepGraph
progDependencyGraph DatalogProgram{..} = G.insEdges (edges ++ apply_edges) g1
    where
    g0 = G.insNodes (zip [0..] $ map DepNodeRel $ map name $ M.elems progRelations) G.empty
    indexed_applys = zip [M.size progRelations ..] progApplys
    g1 = G.insNodes (map (mapSnd DepNodeApply) indexed_applys) g0
    relidx rel = M.findIndex rel progRelations
    edges = concatMap (\Rule{..} ->
                        concatMap (\RuleLHS{..} ->
                                    mapMaybe (\case
                                               RHSLiteral pol a' | not (atomIsDelayed a') -> Just (relidx $ atomRelation a', relidx $ atomRelation lhsAtom, pol)
                                               _ -> Nothing)
                                             ruleRHS)
                                  ruleLHS)
                      progRules
    apply_edges = concatMap (\(idx, Apply{..}) ->
                             let inp_rels = filter (isUpper . head) applyInputs in
                             map (\i -> (relidx i, idx, True)) inp_rels ++
                             map (\o -> (idx, relidx o, True)) applyOutputs ++
                             map (\o -> (idx, relidx o, False)) applyOutputs)
                  indexed_applys

depGraphToDot :: DepGraph -> String
depGraphToDot gr =
  show $ GV.runDotCode $ GV.toDot $ GV.graphToDot params gr
  where
    params = GV.nonClusteredParams {
        GV.fmtNode = \(_, l) -> [GV.Label $ GV.StrLabel $ T.pack $ show l]
    }

-- convert all intermediate relations into output relations
progOutputInternalRelations :: DatalogProgram -> DatalogProgram
progOutputInternalRelations d =
  d { progRelations = M.map
      (\r -> r { relRole = if relRole r == RelInternal
                           then RelOutput else relRole r }) $ progRelations d }

-- create an output relation for each input relation
progMirrorInputRelations :: DatalogProgram -> String -> DatalogProgram
progMirrorInputRelations d prefix =
  let
    output_relname rel = scoped (nameScope rel) (prefix ++ nameLocalStr rel)
    inputRels = M.toList $ M.filter (\r -> relRole r == RelInput) $ progRelations d
    relCopies = map (\(n,r) -> (output_relname n, r { relRole = RelOutput,
                                                 relName = output_relname (relName r),
                                                 relPrimaryKey = Nothing
                                               })) $ inputRels
    makeRule relName relation = Rule { rulePos = relPos relation,
                                       ruleModule = nameScope relName,
                                       ruleLHS = [RuleLHS {
                                                    lhsPos = relPos relation,
                                                    lhsAtom = Atom { atomPos = relPos relation,
                                                                     atomRelation = output_relname relName,
                                                                     atomDelay = delayZero,
                                                                     atomDiff = False,
                                                                     atomVal = eVar "x"
                                                                   },
                                                    lhsLocation = Nothing
                                                  }],
                                       ruleRHS = [RHSLiteral { rhsPolarity = True,
                                                               rhsAtom = Atom { atomPos = relPos relation,
                                                                                atomRelation = relName,
                                                                                atomDelay = delayZero,
                                                                                atomDiff = False,
                                                                                atomVal = eVar "x"
                                                                              }}]}
    rules = map (\(n,r) -> makeRule n r) inputRels
  in d { progRelations = M.union (progRelations d) $ M.fromList relCopies,
         progRules     = (progRules d) ++ rules }

-- Perform datalog program transform by injecting debugging hooks
progInjectDebuggingHooks :: DatalogProgram -> DatalogProgram
progInjectDebuggingHooks d =
  let
    rules = progRules d
    updatedRules = [(rules !! i) {ruleRHS = debugUpdateRHSRules d i (rules !! i)}  | i <- [0..length rules - 1]]
  in d { progRules = updatedRules }
